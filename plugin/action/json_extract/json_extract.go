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

The plugin extracts fields on the go and can work with incomplete JSON (e.g. it was cut by max size limit).
If the field value is incomplete JSON string, fields can be extracted from the remaining part which must be the first half of JSON,
e.g. fields can be extracted from `{"service":"test","message":"long message"`, but not from `"service":"test","message:"long message"}`
because the start as a valid JSON matters.

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
        - meta
        - flags
    ...
```
The original event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"error\":{\"code\":2,\"args\":[]},\"meta\":{\"service\":\"my-service\",\"pod\":\"my-service-5c4dfcdcd4-4v5zw\"},\"flags\":[\"flag1\",\"flag2\"]}",
  "time": "2024-03-01T10:49:28.263317941Z"
}
```
The resulting event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"error\":{\"code\":2,\"args\":[]},\"meta\":{\"service\":\"my-service\",\"pod\":\"my-service-5c4dfcdcd4-4v5zw\"},\"flags\":[\"flag1\",\"flag2\"]}",
  "time": "2024-03-01T10:49:28.263317941Z",
  "code": 2,
  "level": "error",
  "meta": {
    "service": "my-service",
    "pod": "my-service-5c4dfcdcd4-4v5zw"
  },
  "flags": ["flag1", "flag2"]
}
```
---
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_extract
      field: log
      extract_fields:
        - extract1
        - extract2
      prefix: ext_
    ...
```
The original event:
```json
{
  "log": "{\"level\":\"error\",\"extract1\":\"data1\",\"extract2\":\"long message ...",
  "time": "2024-03-01T10:49:28.263317941Z"
}
```
The resulting event:
```json
{
  "log": "{\"level\":\"error\",\"extract1\":\"data1\",\"extract2\":\"long message ...",
  "time": "2024-03-01T10:49:28.263317941Z",
  "ext_extract1": "data1"
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
	// > Field to extract.
	// >> ⚠ DEPRECATED. Use `extract_fields` instead.
	ExtractField  cfg.FieldSelector `json:"extract_field" parse:"selector"` // *
	ExtractField_ []string

	// > @3@4@5@6
	// >
	// > Fields to extract.
	ExtractFields []cfg.FieldSelector `json:"extract_fields" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > A prefix to add to extracted field keys.
	Prefix string `json:"prefix" default:""` // *
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.decoder = &jx.Decoder{}

	p.extractFields = newPathTree()
	dupl := false
	for _, f := range p.config.ExtractFields {
		if f == p.config.ExtractField {
			dupl = true
		}
		p.extractFields.add(cfg.ParseFieldSelector(string(f)))
	}
	if !dupl {
		p.extractFields.add(p.config.ExtractField_)
	}

	if len(p.extractFields.root.children) == 0 {
		params.Logger.Fatal("extract fields are empty")
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	p.decoder.ResetBytes(jsonNode.AsBytes())
	extract(event.Root, p.decoder, p.extractFields.root.children, p.config.Prefix, false)
	return pipeline.ActionPass
}

// extract extracts fields from decoder and adds it to the root.
//
// [skipAddField] flag is required for proper benchmarking.
func extract(root *insaneJSON.Root, d *jx.Decoder, fields pathNodes, prefix string, skipAddField bool) {
	objIter, err := d.ObjIter()
	if err != nil {
		return
	}

	processed := len(fields)
	for objIter.Next() {
		// find the field at the current depth
		n := fields.find(string(objIter.Key()))
		if n == nil {
			if err = d.Skip(); err != nil {
				break
			}
			continue
		}

		if len(n.children) == 0 { // last field in path, add to root
			if skipAddField {
				if err = d.Skip(); err != nil {
					break
				}
			} else {
				if err = addField(root, prefix+n.data, d); err != nil {
					break
				}
			}
		} else { // go deep
			// Capture calls f and then rolls back to state before call
			_ = d.Capture(func(d *jx.Decoder) error {
				// recursively extract child fields
				extract(root, d, n.children, prefix, skipAddField)
				return nil
			})
			// skip the current field because we have processed it
			// and rolled back the state of the decoder
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

func addField(root *insaneJSON.Root, field string, d *jx.Decoder) error {
	switch d.Next() {
	case jx.Number:
		num, err := d.Num()
		if err != nil {
			return err
		}
		var (
			intVal   int64
			floatVal float64
		)
		intVal, err = num.Int64()
		if err == nil {
			root.AddFieldNoAlloc(root, field).MutateToInt64(intVal)
		} else {
			floatVal, err = num.Float64()
			if err == nil {
				root.AddFieldNoAlloc(root, field).MutateToFloat(floatVal)
			}
		}
		if err != nil {
			return err
		}
	case jx.String:
		s, err := d.StrBytes()
		if err != nil {
			return err
		}
		root.AddFieldNoAlloc(root, field).MutateToBytesCopy(root, s)
	case jx.Null:
		err := d.Null()
		if err != nil {
			return err
		}
		root.AddFieldNoAlloc(root, field).MutateToNull()
	case jx.Bool:
		b, err := d.Bool()
		if err != nil {
			return err
		}
		root.AddFieldNoAlloc(root, field).MutateToBool(b)
	case jx.Object, jx.Array:
		raw, err := d.Raw()
		if err != nil {
			return err
		}
		root.AddFieldNoAlloc(root, field).MutateToJSON(root, raw.String())
	default:
		return d.Skip()
	}
	return nil
}
