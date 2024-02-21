package json_extract

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestJsonExtract(t *testing.T) {
	cases := []struct {
		name   string
		config *Config
		in     string
		want   string
	}{
		{
			name: "extract_string",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3,"extracted":"text"}`,
		},
		{
			name: "extract_int",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"extracted\":5,\"test\":\"test_value\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"extracted\":5,\"test\":\"test_value\"}","field3":3,"extracted":5}`,
		},
		{
			name: "extract_float",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":95.6}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":95.6}","field3":3,"extracted":95.6}`,
		},
		{
			name: "extract_bool",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":true}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":true}","field3":3,"extracted":true}`,
		},
		{
			name: "extract_object",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":{\"ext1\":\"val1\",\"ext2\":25}}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":{\"ext1\":\"val1\",\"ext2\":25}}","field3":3,"extracted":{"ext1":"val1","ext2":25}}`,
		},
		{
			name: "nested_fields",
			config: &Config{
				Field:          "log.json_field",
				ExtractedField: "extracted.extracted2",
			},
			in:   `{"field1":"value1","log":{"json_field":"{\"test\":\"test_value\",\"extracted\":{\"extracted1\":\"text\",\"extracted2\":15}}","field3":3}}`,
			want: `{"field1":"value1","log":{"json_field":"{\"test\":\"test_value\",\"extracted\":{\"extracted1\":\"text\",\"extracted2\":15}}","field3":3},"extracted2":15}`,
		},
		{
			name: "field_not_exists",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","field3":3}`,
			want: `{"field1":"value1","field3":3}`,
		},
		{
			name: "extracted_field_not_exists",
			config: &Config{
				Field:          "json_field",
				ExtractedField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\"}","field3":3}`,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := test.NewConfig(tt.config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

			wg := &sync.WaitGroup{}
			wg.Add(1)

			output.SetOutFn(func(e *pipeline.Event) {
				assert.Equal(t, tt.want, e.Root.EncodeToString(), "wrong event root")
				wg.Done()
			})

			input.In(0, "test.log", 0, []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}

func TestFindEndJsonObject(t *testing.T) {
	cases := []struct {
		name         string
		jsonBrackets string
		wantIdx      int
	}{
		{
			name:         "valid",
			jsonBrackets: "{ {}[] }",
			wantIdx:      7,
		},
		{
			name:         "valid_simple",
			jsonBrackets: "{}",
			wantIdx:      1,
		},
		{
			name:         "valid_multi",
			jsonBrackets: "{ [][]{ {} } } {[]}",
			wantIdx:      13,
		},
		{
			name:         "invalid_close",
			jsonBrackets: "{ {} []",
			wantIdx:      -1,
		},
		{
			name:         "invalid_open",
			jsonBrackets: "{ { [] }",
			wantIdx:      -1,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx := findEndJsonObject([]byte(tt.jsonBrackets))
			require.Equal(t, tt.wantIdx, idx)
		})
	}
}

func TestParseJsonValue(t *testing.T) {
	cases := []struct {
		name      string
		json      string
		wantVal   any
		wantIsObj bool
	}{
		{
			name:      "object",
			json:      `{"field1":"value1","field2":"value2"}`,
			wantVal:   `{"field1":"value1","field2":"value2"}`,
			wantIsObj: true,
		},
		{
			name:    "string_valid",
			json:    `"value"`,
			wantVal: "value",
		},
		{
			name:    "string_invalid_1",
			json:    `"value`,
			wantVal: nil,
		},
		{
			name:    "string_invalid_2",
			json:    `value"`,
			wantVal: nil,
		},
		{
			name:    "int",
			json:    "123",
			wantVal: 123,
		},
		{
			name:    "float",
			json:    "123.321",
			wantVal: 123.321,
		},
		{
			name:    "bool_1",
			json:    "true",
			wantVal: true,
		},
		{
			name:    "bool_2",
			json:    "false",
			wantVal: false,
		},
		// not supports
		{
			name:    "array",
			json:    "[1, 2, 3]",
			wantVal: nil,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			val, isObj := parseJsonValue([]byte(tt.json))
			require.Equal(t, tt.wantVal, val)
			require.Equal(t, tt.wantIsObj, isObj)
		})
	}
}

func genFields(count int) string {
	var sb strings.Builder
	for i := 0; i < count; i++ {
		sb.WriteString(fmt.Sprintf(`"field_%d":"val_%d",`, count, count))
	}
	return sb.String()
}

const extractBenchJsonFormat = `{%s"level":"info","ts":"2024-02-21T08:31:24.621Z","message":"some message","traceID":"123e456e789e0123","rule_name":"simple_trace"}`

var extractedField = []string{"level"}

var extractBenchCases = []struct {
	json string
}{
	{
		json: fmt.Sprintf(extractBenchJsonFormat, genFields(0)),
	},
	{
		json: fmt.Sprintf(extractBenchJsonFormat, genFields(10)),
	},
	{
		json: fmt.Sprintf(extractBenchJsonFormat, genFields(100)),
	},
	{
		json: fmt.Sprintf(extractBenchJsonFormat, genFields(500)),
	},
}

func BenchmarkExtractParse(b *testing.B) {
	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_length_%d", len(benchCase.json))

		var data []byte
		fieldBuf := &bytes.Buffer{}

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				data = data[:0]
				data = append(data, benchCase.json...)
				data = extract(data, extractedField, fieldBuf)
				parseJsonValue(data)
			}
		})
	}
}

func BenchmarkExtractParseInsane(b *testing.B) {
	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_length_%d", len(benchCase.json))

		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				node, _ := root.DecodeStringAdditional(benchCase.json)
				node.Dig(extractedField...)
			}
		})
	}
}
