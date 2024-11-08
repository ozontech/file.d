package modify

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/cfg/substitution"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It modifies the content for a field or add new field. It works only with strings.
You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`cfg.Substitution`.
When `_skip_empty` is set to `true`, the field won't be modified/added in the case of field value is empty.

> Note: When used to add new nested fields, each child field is added step by step, which can cause performance issues.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: modify
      my_object.field.subfield: value is ${another_object.value}.
    ...
```

The resulting event could look like:
```
{
  "my_object": {
    "field": {
      "subfield":"value is 666."
    }
  },
  "another_object": {
    "value": 666
  }
```
}*/

/*{ filters
Sometimes it is required to extract certain data from fields and for that purpose filter chains were added.
Filters are added one after another using pipe '|' symbol and they are applied to the last value in the chain.

For example, in expression `${field|re("(test-pod-\w+)",-1,[1],",")|re("test-pod-(\w+)",-1,[1],",")}` first the value of 'field' is retrieved,
then the data extracted using first regular expression and formed into a new string, then the second regular expression is applied
and its result is formed into a value to be put in modified field.

Currently available filters are:

+ `regex filter` - `re(regex string, limit int, groups []int, separator string[, emptyOnNotMatched bool])`, filters data using `regex`, extracts `limit` occurrences,
takes regex groups listed in `groups` list, and if there are more than one extracted element concatenates result using `separator`.
Negative value of `limit` means all occurrences are extracted, `limit` 0 means no occurrences are extracted, `limit` greater than 0 means
at most `limit` occurrences are extracted.
Optional flag `emptyOnNotMatched` allows to returns empty string if no matches occurred for regex.

+ `trim filter` - `trim(mode string, cutset string)`, trims data by the `cutset` substring. Available modes are `all` - trim both sides,
`left` - trim only left, `right` - trim only right.

+ `trim-to filter` - `trim_to(mode string, cutset string)`, trims data to `cutset` substring. Available modes are `all` - trim both sides,
`left` - trim only left, `right` - trim only right.

### Examples

**Example re #1**

Data: `{"message":"info: something happened"}`

Substitution: `level: ${message|re("(\w+):.*",-1,[1],",")}`

Result: `{"message":"info: something happened","level":"info"}`

**Example re #2**

Data: `{"message":"re1 re2 re3 re4"}`

Substitution: `extracted: ${message|re("(re\d+)",2,[1],",")}`

Result: `{"message":"re1 re2 re3 re4","extracted":"re1,re2"}`

**Example re #3**

Data: `{"message":"service=service-test-1 exec took 200ms"}`

Substitution: `took: ${message|re("service=([A-Za-z0-9_\-]+) exec took (\d+\.?\d*(?:ms|s|m|h))",-1,[2],",")}`

Result: `{"message":"service=service-test-1 exec took 200ms","took":"200ms"}`

**Example re #4**

Data: `{"message":"message without matching re"}`

Substitution: `extracted: ${message|re("test",1,[1],",",true)}`

Result: `{"message":"message without matching re","extracted":""}`

**Example trim**

Data: `{"message":"{\"service\":\"service-test-1\",\"took\":\"200ms\"}\n"}`

Substitution: `message: ${message|trim("right","\n")}`

Result: `{"message":"{\"service\":\"service-test-1\",\"took\":\"200ms\"}"}`

**Example trim-to**

Data: `{"message":"some data {\"service\":\"service-test-1\",\"took\":\"200ms\"} some data"}`

Substitution: `message: ${message|trim_to("left","{")|trim_to("right","}")}`

Result: `{"message":"{\"service\":\"service-test-1\",\"took\":\"200ms\"}"}`

}*/

const (
	filterBufInitSize = 1024

	skipEmptyKey = "_skip_empty"
)

type fieldOp struct {
	field []string
	ops   []substitution.SubstitutionOp
}

type Plugin struct {
	config    *Config
	logger    *zap.Logger
	fieldOps  []fieldOp
	skipEmpty bool
	buf       []byte
	fieldBuf  []byte
}

type Config map[string]string

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "modify",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.fieldOps = make([]fieldOp, 0, len(*p.config))
	p.logger = params.Logger.Desugar()

	filtersBuf := make([]byte, 0, filterBufInitSize)
	for key, value := range *p.config {
		if key == skipEmptyKey {
			p.skipEmpty = value == "true"
			continue
		}

		// if there are field filters in substitutions, they will have single buffer for all
		// substitution ops in this plugin
		ops, err := substitution.ParseSubstitution(value, filtersBuf, p.logger)
		if err != nil {
			p.logger.Fatal("can't parse substitution", zap.Error(err))
		}

		if len(ops) == 0 {
			continue
		}

		p.fieldOps = append(p.fieldOps, fieldOp{
			field: cfg.ParseFieldSelector(key),
			ops:   ops,
		})
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for _, fo := range p.fieldOps {
		p.buf = p.buf[:0]
		for _, op := range fo.ops {
			switch op.Kind {
			case substitution.SubstitutionOpKindRaw:
				p.buf = append(p.buf, op.Data[0]...)
			case substitution.SubstitutionOpKindField:
				p.fieldBuf = p.fieldBuf[:0]
				fieldData := event.Root.Dig(op.Data...).AsBytes()
				p.fieldBuf = append(p.fieldBuf, fieldData...)
				for i := 0; i < len(op.Filters); i++ {
					p.fieldBuf = op.Filters[i].Apply(p.fieldBuf, p.fieldBuf)
				}
				p.buf = append(p.buf, p.fieldBuf...)
			default:
				p.logger.Panic("unknown substitution kind", zap.Int("substitution_kind", int(op.Kind)))
			}
		}

		if p.skipEmpty && len(p.buf) == 0 {
			continue
		}
		pipeline.CreateNestedField(event.Root, fo.field).MutateToBytesCopy(event.Root, p.buf)
	}

	return pipeline.ActionPass
}
