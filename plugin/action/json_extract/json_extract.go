package json_extract

import (
	"github.com/go-faster/jx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It extracts fields from JSON-encoded event field and adds extracted fields to the event root.
> If extracted field already exists in the event root, it will be overridden.
}*/

/*{ examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_extract
      field: log
      extract_fields:
        - error.code
        - level
    ...
```
The original event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"service\":\"my-service\",\"error\":{\"code\":2,\"args\":[]}}",
  "time": "2024-03-01T10:49:28.263317941Z"
}
```
The resulting event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"service\":\"my-service\",\"error\":{\"code\":2,\"args\":[]}}",
  "time": "2024-03-01T10:49:28.263317941Z",
  "code": 2,
  "level": "error"
}
```
}*/

/*{ benchmarks
Performance comparison of `json_extract` and `json_decode` plugins.
`json_extract` on average 2.5 times faster than `json_decode` and
doesn't allocate memory during the extract process.

### Extract 1 field
| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 309           | 300                    | 560                   |
| 2109          | 2570                   | 7250                  |
| 10909         | 13550                  | 34250                 |
| 21909         | 26000                  | 67940                 |
| 237909        | 262500                 | 741530                |

### Extract 5 fields
| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 309           | 450                    | 685                   |
| 2109          | 2990                   | 7410                  |
| 10909         | 14540                  | 35000                 |
| 21909         | 28340                  | 69950                 |
| 237909        | 286600                 | 741600                |
}*/

type Plugin struct {
	config *Config

	extractFields *pathTree
	decoder       *jx.Decoder
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field from which to extract. Must be a string.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Fields to extract.
	ExtractFields []cfg.FieldSelector `json:"extract_fields" slice:"true" required:"true"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "json_extract",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.decoder = &jx.Decoder{}

	p.extractFields = newPathTree()
	for _, f := range p.config.ExtractFields {
		p.extractFields.add(cfg.ParseFieldSelector(string(f)))
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	p.decoder.ResetBytes(jsonNode.AsBytes())
	extract(event.Root, p.decoder, p.extractFields.root.children, false)
	return pipeline.ActionPass
}

// extract extracts fields from decoder and adds it to the root.
//
// [skipAddField] flag is required for proper benchmarking.
func extract(root *insaneJSON.Root, d *jx.Decoder, fields pathNodes, skipAddField bool) {
	objIter, err := d.ObjIter()
	if err != nil {
		return
	}

	processed := len(fields)
	for objIter.Next() {
		n := fields.find(string(objIter.Key()))
		if n == nil {
			if err = d.Skip(); err != nil {
				break
			}
			continue
		}

		// last field in path, add to root
		if len(n.children) == 0 {
			if skipAddField {
				_ = d.Skip()
			} else {
				addField(root, n.data, d)
			}
		} else { // go deep
			_ = d.Capture(func(d *jx.Decoder) error {
				extract(root, d, n.children, skipAddField)
				return nil
			})
			if err = d.Skip(); err != nil {
				break
			}
		}

		processed--
		if processed == 0 {
			break
		}
	}
}

func addField(root *insaneJSON.Root, field string, d *jx.Decoder) {
	switch d.Next() {
	case jx.Number:
		num, _ := d.Num()
		intVal, err := num.Int64()
		if err == nil {
			root.AddFieldNoAlloc(root, field).MutateToInt64(intVal)
		} else {
			floatVal, err := num.Float64()
			if err == nil {
				root.AddFieldNoAlloc(root, field).MutateToFloat(floatVal)
			}
		}
	case jx.String:
		s, _ := d.StrBytes()
		root.AddFieldNoAlloc(root, field).MutateToBytesCopy(root, s)
	case jx.Null:
		_ = d.Null()
		root.AddFieldNoAlloc(root, field).MutateToNull()
	case jx.Bool:
		b, _ := d.Bool()
		root.AddFieldNoAlloc(root, field).MutateToBool(b)
	case jx.Object, jx.Array:
		raw, _ := d.Raw()
		root.AddFieldNoAlloc(root, field).MutateToJSON(root, raw.String())
	default:
		_ = d.Skip()
	}
}
