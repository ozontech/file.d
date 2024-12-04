package json_extract

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-faster/jx"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
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
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3,"extracted":"text"}`,
		},
		{
			name: "extract_int",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"extracted\":5,\"test\":\"test_value\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"extracted\":5,\"test\":\"test_value\"}","field3":3,"extracted":5}`,
		},
		{
			name: "extract_float",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":95.6}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":95.6}","field3":3,"extracted":95.6}`,
		},
		{
			name: "extract_bool",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":true}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":true}","field3":3,"extracted":true}`,
		},
		{
			name: "extract_null",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"extracted\":null,\"test\":\"test_value\"}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"extracted\":null,\"test\":\"test_value\"}","field3":3,"extracted":null}`,
		},
		{
			name: "extract_object",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":{\"ext1\":\"val1\",\"ext2\":25}}","field3":3}`,
			want: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":{\"ext1\":\"val1\",\"ext2\":25}}","field3":3,"extracted":{"ext1":"val1","ext2":25}}`,
		},
		{
			name: "nested_fields",
			config: &Config{
				Field:        "log.json_field",
				ExtractField: "extracted.extracted2",
			},
			in:   `{"field1":"value1","log":{"json_field":"{\"test\":\"test_value\",\"extracted\":{\"extracted1\":\"text\",\"extracted2\":15}}","field3":3}}`,
			want: `{"field1":"value1","log":{"json_field":"{\"test\":\"test_value\",\"extracted\":{\"extracted1\":\"text\",\"extracted2\":15}}","field3":3},"extracted2":15}`,
		},
		{
			name: "field_not_exists",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in:   `{"field1":"value1","field3":3}`,
			want: `{"field1":"value1","field3":3}`,
		},
		{
			name: "extracted_field_not_exists",
			config: &Config{
				Field:        "test1",
				ExtractField: "extracted",
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

			input.In(0, "test.log", test.Offset(0), []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}

func genFields(count int) string {
	var sb strings.Builder
	for i := 0; i < count; i++ {
		sb.WriteString(fmt.Sprintf(`"field_%d":"val_%d",`, i, i))
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
	{
		json: fmt.Sprintf(extractBenchJsonFormat, genFields(1000)),
	},
}

func BenchmarkExtractObj(b *testing.B) {
	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_length_%d", len(benchCase.json))

		b.Run(name, func(b *testing.B) {
			node := insaneJSON.Spawn()
			d := &jx.Decoder{}
			for i := 0; i < b.N; i++ {
				d.ResetBytes(pipeline.StringToByteUnsafe(benchCase.json))
				// remove allocs for adding new fields to root by passing `skipAddField` flag for correct benching
				extract(node, d, extractedField, 0, true)
			}
			insaneJSON.Release(node)
		})
	}
}

func BenchmarkInsaneDecodeDig(b *testing.B) {
	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_length_%d", len(benchCase.json))

		b.Run(name, func(b *testing.B) {
			node := insaneJSON.Spawn()
			for i := 0; i < b.N; i++ {
				_ = node.DecodeString(benchCase.json)
				node.Dig(extractedField...)
			}
			insaneJSON.Release(node)
		})
	}
}
