package modify

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

const filterBufInitSize = 1024

/*{ introduction
It modifies the content for a field. It works only with strings.
You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`cfg.Substitution`.

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

type Plugin struct {
	config   *Config
	logger   *zap.Logger
	ops      map[string][]cfg.SubstitutionOp
	buf      []byte
	fieldBuf []byte
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
	p.ops = make(map[string][]cfg.SubstitutionOp)
	p.logger = params.Logger.Desugar()

	filtersBuf := make([]byte, 0, filterBufInitSize)
	for key, value := range *p.config {
		// if there are field filters in substitutions, they will have single buffer for all
		// substitution ops in this plugin
		ops, err := cfg.ParseSubstitution(value, filtersBuf, p.logger)
		if err != nil {
			p.logger.Fatal("can't parse substitution", zap.Error(err))
		}

		if len(ops) == 0 {
			continue
		}

		p.ops[key] = ops
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for field, list := range p.ops {
		p.buf = p.buf[:0]
		for _, op := range list {
			switch op.Kind {
			case cfg.SubstitutionOpKindRaw:
				p.buf = append(p.buf, op.Data[0]...)
			case cfg.SubstitutionOpKindField:
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

		event.Root.AddFieldNoAlloc(event.Root, field).MutateToBytesCopy(event.Root, p.buf)
	}

	return pipeline.ActionPass
}
