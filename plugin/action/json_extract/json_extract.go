package json_extract

import (
	"bytes"

	"github.com/go-faster/jx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It extracts a field from JSON-encoded event field and adds extracted field to the event root.
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
      extract_field: error.code
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
  "code": 2
}
```
}*/

/*{ benchmarks
Performance comparison of `json_extract` and `json_decode` plugins.
`json_extract` on average 3 times faster than `json_decode`.

| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 129           | 33                     | 176                   |
| 309           | 264                    | 520                   |
| 2109          | 2263                   | 6778                  |
| 10909         | 11289                  | 32205                 |
| 21909         | 23277                  | 62819                 |
}*/

type Plugin struct {
	config  *Config
	decoder *jx.Decoder
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
	// > Field to extract.
	ExtractField  cfg.FieldSelector `json:"extract_field" parse:"selector" required:"true"` // *
	ExtractField_ []string
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
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	p.decoder.ResetBytes(jsonNode.AsBytes())
	extract(event.Root, p.decoder, p.config.ExtractField_, 0, false)
	return pipeline.ActionPass
}

// extract extracts field from decoder and adds it to the root.
// `skipAddField` flag is required for proper benchmarking.
func extract(root *insaneJSON.Root, d *jx.Decoder, field []string, depth int, skipAddField bool) {
	objIter, err := d.ObjIter()
	if err != nil {
		return
	}

	for objIter.Next() {
		if bytes.Equal(objIter.Key(), pipeline.StringToByteUnsafe(field[depth])) {
			if depth == len(field)-1 { // add field
				if skipAddField {
					_ = d.Skip()
				} else {
					addField(root, field[depth], d)
				}
			} else { // go deep
				raw, err := d.Raw()
				if err != nil {
					break
				}
				d.ResetBytes(raw)
				extract(root, d, field, depth+1, skipAddField)
			}
			break
		} else if err = d.Skip(); err != nil {
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
