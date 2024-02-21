package json_extract

import (
	"bytes"
	"strconv"
	"unicode"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It extracts a field value from JSON-encoded event field and adds a new field with this value to the event root.
Supports: object, string, int, float and bool values.
}*/

type Plugin struct {
	config *Config

	dataBuf  []byte
	fieldBuf *bytes.Buffer
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
	// > The field whose value will be extracted. Must be a string.
	ExtractedField  cfg.FieldSelector `json:"extracted_field" parse:"selector" required:"true"` // *
	ExtractedField_ []string
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
	p.fieldBuf = &bytes.Buffer{}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	p.dataBuf = p.dataBuf[:0]
	p.dataBuf = append(p.dataBuf, jsonNode.AsString()...)

	p.dataBuf = extract(p.dataBuf, p.config.ExtractedField_, p.fieldBuf)
	if len(p.dataBuf) == 0 {
		return pipeline.ActionPass
	}

	val, isObj := parseJsonValue(p.dataBuf)
	if val == nil {
		return pipeline.ActionPass
	}

	node := event.Root.AddFieldNoAlloc(event.Root, p.config.ExtractedField_[len(p.config.ExtractedField_)-1])
	switch v := val.(type) {
	case string:
		if isObj {
			node.MutateToJSON(event.Root, v)
		} else {
			node.MutateToString(v)
		}
	case int:
		node.MutateToInt(v)
	case float64:
		node.MutateToFloat(v)
	case bool:
		node.MutateToBool(v)
	}

	return pipeline.ActionPass
}

func extract(data []byte, field []string, fieldBuf *bytes.Buffer) []byte {
	resetAndReturn := func() []byte {
		data = data[:0]
		return data
	}

	for i, f := range field {
		// formatted field = "<field>":
		fieldBuf.Reset()
		fieldBuf.WriteByte('"')
		fieldBuf.WriteString(f)
		fieldBuf.WriteString(`":`)

		fieldIdx := bytes.Index(data, fieldBuf.Bytes())
		if fieldIdx == -1 {
			return resetAndReturn()
		}

		data = bytes.TrimLeft(data[fieldIdx+fieldBuf.Len():], " ")

		// field must be an object
		if i < len(field)-1 && data[0] != '{' {
			return resetAndReturn()
		}

		var endIdx int
		if data[0] == '{' { // object
			endIdx = findEndJsonObject(data)
		} else { // value
			endIdx = bytes.IndexAny(data, ",}")
		}

		if endIdx == -1 {
			return resetAndReturn()
		}

		if data[0] == '{' {
			endIdx++
		}
		data = data[:endIdx]
	}

	return data
}

// findEndJsonObject finds end of json object.
// data must starts with '{'.
func findEndJsonObject(data []byte) int {
	offset := 1
	brackets := 1

	for {
		idx := bytes.IndexAny(data[offset:], "{}")
		if idx == -1 {
			return -1
		}

		if data[offset:][idx] == '{' {
			brackets++
		} else {
			brackets--
			if brackets == 0 {
				return offset + idx
			}
		}
		offset += idx + 1
	}
}

// parseJsonValue parses json data to value.
// Supports: object, string, int, float64, bool.
// 2nd return value is used with string values that are represent json-objects.
func parseJsonValue(data []byte) (any, bool) {
	if data[0] == '{' {
		return pipeline.ByteToStringUnsafe(bytes.Clone(data)), true
	}
	if data[0] == '"' {
		end := bytes.IndexByte(data[1:], '"')
		if end == -1 {
			return nil, false
		}
		return pipeline.ByteToStringUnsafe(bytes.Clone(data[1 : end+1])), false
	}
	str := pipeline.ByteToStringUnsafe(data)
	if str[0] == '+' || str[0] == '-' || unicode.IsDigit(rune(str[0])) {
		if v, err := strconv.ParseInt(str, 10, 32); err == nil {
			return int(v), false
		}
		if v, err := strconv.ParseFloat(str, 64); err == nil {
			return v, false
		}
		return nil, false
	}
	if v, err := strconv.ParseBool(str); err == nil {
		return v, false
	}
	return nil, false
}
