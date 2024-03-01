package json_extract

import (
	"bytes"

	"github.com/go-faster/jx"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ introduction
It extracts a fields from JSON-encoded event field and adds extracted fields to the event root.
Supports: `object`, `string`, `int`, `float`, `bool` and `null` values.
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
  "level": "error",
  "code": 2
}
```
}*/

/*{ benchmarks
Performance comparison of `json_extract` and  `json_decode` plugins. Each bench named by length of json-field.

**json_extract**

`$ go test -bench=BenchmarkExtract -benchmem ./plugin/action/json_extract/...`

|                                         |          |             |        |             |
|-----------------------------------------|----------|-------------|--------|-------------|
| BenchmarkExtractObj/json_length_129-8   | 24082884 | 50.99 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_329-8   | 4063610  | 303.0 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_2329-8  | 498919   | 2383 ns/op  | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_11129-8 | 103920   | 11756 ns/op | 0 B/op | 0 allocs/op |

**json_decode**

`$ go test -bench=BenchmarkInsane -benchmem ./plugin/action/json_extract/...`

|                                              |         |             |        |             |
|----------------------------------------------|---------|-------------|--------|-------------|
| BenchmarkInsaneDecodeDig/json_length_129-8   | 6968088 | 171.7 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_329-8   | 2122134 | 576.8 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_2329-8  | 250518  | 4808 ns/op  | 8 B/op | 1 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_11129-8 | 54476   | 25769 ns/op | 8 B/op | 1 allocs/op |
}*/

type Plugin struct {
	config    *Config
	allFields [][]string // list of all fields (equals to config.ExtractFields)

	rootDecoder objDecoder
	objDecoders objDecoders

	idxBuf []int
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

	p.rootDecoder = objDecoder{d: &jx.Decoder{}}

	tmpSet := make(map[string]struct{})
	tmpBuf := make([][]byte, 0)
	for _, field := range p.config.ExtractFields {
		if fs := cfg.ParseFieldSelector(string(field)); len(fs) > 0 {
			p.allFields = append(p.allFields, fs)

			// root fields without duplicates
			if _, ok := tmpSet[fs[0]]; !ok {
				p.rootDecoder.fields = append(p.rootDecoder.fields, pipeline.StringToByteUnsafe(fs[0]))
				tmpSet[fs[0]] = struct{}{}
			}

			if len(fs) > 1 {
				tmpBuf = tmpBuf[:0]
				for i := 0; i < len(fs); i++ {
					tmpBuf = append(tmpBuf, pipeline.StringToByteUnsafe(fs[i]))
				}
				// find decoder for this obj.
				// for example, if fs=[log, error, level], we need decoder with path=[log, error]
				p.idxBuf = p.objDecoders.findStartsWith(tmpBuf[:len(tmpBuf)-1], p.idxBuf)
				if len(p.idxBuf) == 0 {
					// create new decoder
					od := objDecoder{d: &jx.Decoder{}}
					for i := 0; i < len(tmpBuf)-1; i++ {
						od.path = append(od.path, tmpBuf[i])
					}
					p.objDecoders = append(p.objDecoders, od)
					p.idxBuf = append(p.idxBuf, len(p.objDecoders)-1)
				}
				// add field to exists decoder
				p.objDecoders[p.idxBuf[0]].fields = append(p.objDecoders[p.idxBuf[0]].fields, tmpBuf[len(tmpBuf)-1])
			}
		}
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if len(p.rootDecoder.fields) == 0 {
		return pipeline.ActionPass
	}

	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	p.rootDecoder.d.ResetBytes(jsonNode.AsBytes())

	// fast way for only root fields or single obj decoder
	if len(p.objDecoders) == 0 {
		extractObj(event.Root, p.rootDecoder, 0, false)
		return pipeline.ActionPass
	} else if len(p.objDecoders) == 1 && len(p.rootDecoder.fields) == 1 && bytes.Equal(p.objDecoders[0].path[0], p.rootDecoder.fields[0]) {
		p.objDecoders[0].d.ResetBytes(jsonNode.AsBytes())
		extractObj(event.Root, p.objDecoders[0], 0, false)
		return pipeline.ActionPass
	}

	objIter, err := p.rootDecoder.d.ObjIter()
	if err != nil {
		return pipeline.ActionPass
	}

	processedFields := make(map[int]struct{}, len(p.rootDecoder.fields))
iterLoop:
	for objIter.Next() {
		if len(processedFields) == len(p.rootDecoder.fields) {
			break
		}

		for i := range p.rootDecoder.fields {
			if _, ok := processedFields[i]; ok {
				continue
			}
			if bytes.Equal(objIter.Key(), p.rootDecoder.fields[i]) {
				// find all decoders for this obj
				p.idxBuf = p.objDecoders.findStartsWith([][]byte{p.rootDecoder.fields[i]}, p.idxBuf)

				if len(p.idxBuf) == 0 {
					addField(event.Root, string(p.rootDecoder.fields[i]), p.rootDecoder.d)
				} else {
					raw, err := p.rootDecoder.d.Raw()
					if err != nil {
						continue iterLoop
					}
					for _, idx := range p.idxBuf {
						p.objDecoders[idx].d.ResetBytes(raw)
						extractObj(event.Root, p.objDecoders[idx], 1, false)
					}
				}

				processedFields[i] = struct{}{}
				continue iterLoop
			}
		}

		if err = p.rootDecoder.d.Skip(); err != nil {
			break
		}
	}

	return pipeline.ActionPass
}

type objDecoder struct {
	d      *jx.Decoder
	path   [][]byte // path to obj
	fields [][]byte // fields in obj
}

type objDecoders []objDecoder

// findStartsWith finds all object decoders that starts with specified sub-path and returns their indexes.
// It resets idxBuf before finding.
func (d objDecoders) findStartsWith(subPath [][]byte, idxBuf []int) []int {
	idxBuf = idxBuf[:0]
	for i := range d {
		if len(subPath) > len(d[i].path) {
			continue
		}
		j := 0
		for ; j < len(subPath) && bytes.Equal(d[i].path[j], subPath[j]); j++ {
		}
		if j == len(subPath) {
			idxBuf = append(idxBuf, i)
		}
	}
	return idxBuf
}

// extractObj extracts all fields from objDecoder and adds them to the root.
// `skipAddField` flag is required for proper benchmarking.
func extractObj(root *insaneJSON.Root, od objDecoder, depth int, skipAddField bool) {
	objIter, err := od.d.ObjIter()
	if err != nil {
		return
	}

	processedFields := 0
iterLoop:
	for objIter.Next() {
		if depth == len(od.path) { // collect all fields
			if processedFields == len(od.fields) {
				break
			}
			for i := range od.fields {
				if bytes.Equal(objIter.Key(), od.fields[i]) {
					if skipAddField {
						_ = od.d.Skip()
					} else {
						addField(root, string(od.fields[i]), od.d)
					}
					processedFields++
					continue iterLoop
				}
			}
		} else if bytes.Equal(objIter.Key(), od.path[depth]) { // go deep
			raw, err := od.d.Raw()
			if err != nil {
				break
			}
			od.d.ResetBytes(raw)
			extractObj(root, od, depth+1, skipAddField)
			break
		}

		if err = od.d.Skip(); err != nil {
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
	case jx.Object:
		raw, _ := d.Raw()
		root.AddFieldNoAlloc(root, field).MutateToJSON(root, raw.String())
	default:
		_ = d.Skip()
	}
}
