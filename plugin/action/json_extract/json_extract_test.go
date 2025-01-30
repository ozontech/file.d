package json_extract

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/go-faster/jx"
	"github.com/ozontech/file.d/cfg"
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
		want   map[string]string
	}{
		{
			name: "extract_single_old",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
			},
			in: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3}`,
			want: map[string]string{
				"extracted": "text",
			},
		},
		{
			name: "extract_single_new",
			config: &Config{
				Field: "json_field",
				ExtractFields: []cfg.FieldSelector{
					"extracted",
				},
			},
			in: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3}`,
			want: map[string]string{
				"extracted": "text",
			},
		},
		{
			name: "extract_single_nested",
			config: &Config{
				Field: "log.json_field",
				ExtractFields: []cfg.FieldSelector{
					"extracted.extracted2",
				},
			},
			in: `{"field1":"value1","log":{"json_field":"{\"test\":\"test_value\",\"extracted\":{\"extracted1\":\"text\",\"extracted2\":15}}","field3":3}}`,
			want: map[string]string{
				"extracted2": "15",
			},
		},
		{
			name: "extract_multi",
			config: &Config{
				Field: "json_field",
				ExtractFields: []cfg.FieldSelector{
					"extracted_str",
					"extracted_int",
					"extracted_float",
					"extracted_bool",
					"extracted_null",
					"extracted_obj",
					"extracted_arr",
				},
			},
			in: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted_str\":\"str\",\"extracted_int\":10,\"extracted_float\":123.45,\"extracted_bool\":false,\"extracted_null\":null,\"extracted_obj\":{\"ext1\":\"val1\",\"ext2\":25},\"extracted_arr\":[1,2,3,4,5]}","field3":3}`,
			want: map[string]string{
				"extracted_str":   "str",
				"extracted_int":   "10",
				"extracted_float": "123.45",
				"extracted_bool":  "false",
				"extracted_null":  "null",
				"extracted_obj":   `{"ext1":"val1","ext2":25}`,
				"extracted_arr":   "[1,2,3,4,5]",
			},
		},
		{
			name: "extract_multi_nested",
			config: &Config{
				Field: "json_field",
				ExtractFields: []cfg.FieldSelector{
					"ext1.ext2.ext3.ext4",
					"ext1.ext5",
					"ext6",
					"ext1.ext2.ext7",
				},
			},
			in: `{"field1":"value1","json_field":"{\"ext1\":{\"ext2\":{\"ext3\":{\"ext4\":\"test4\",\"ext5\":10},\"ext7\":\"test7\"},\"ext5\":\"test5\"},\"ext2\":2,\"ext6\":\"test6\"}","field3":3}`,
			want: map[string]string{
				"ext4": "test4",
				"ext5": "test5",
				"ext6": "test6",
				"ext7": "test7",
			},
		},
		{
			name: "field_not_exists",
			config: &Config{
				Field: "json_field",
				ExtractFields: []cfg.FieldSelector{
					"extracted",
				},
			},
			in: `{"field1":"value1","field3":3}`,
			want: map[string]string{
				"extracted": "",
			},
		},
		{
			name: "extracted_field_not_exists",
			config: &Config{
				Field: "json_field",
				ExtractFields: []cfg.FieldSelector{
					"extracted",
				},
			},
			in: `{"field1":"value1","json_field":"{\"test\":\"test_value\"}","field3":3}`,
			want: map[string]string{
				"extracted": "",
			},
		},
		{
			name: "extracted_field_duple",
			config: &Config{
				Field:        "json_field",
				ExtractField: "extracted",
				ExtractFields: []cfg.FieldSelector{
					"extracted",
				},
			},
			in: `{"field1":"value1","json_field":"{\"test\":\"test_value\",\"extracted\":\"text\"}","field3":3}`,
			want: map[string]string{
				"extracted": "text",
			},
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
				for k, v := range tt.want {
					node := e.Root.Dig(k)
					got := ""
					if node != nil && v != "" && (v[0] == '[' || v[0] == '{') {
						got = node.EncodeToString()
					} else {
						got = node.AsString()
					}
					assert.Equal(t, v, got, "wrong event value with key %q", k)
				}
				wg.Done()
			})

			input.In(0, "test.log", test.NewOffset(0), []byte(tt.in))

			wg.Wait()
			p.Stop()
		})
	}
}

func genBenchFields(count int) string {
	var sb strings.Builder
	for i := 0; i < count; i++ {
		sb.WriteString(fmt.Sprintf(`"field_%d":"val_%d",`, i, i))
	}
	return sb.String()
}

const extractBenchJsonFormat = `{%s"level":"info","ts":"2024-02-21T08:31:24.621Z","message":"some message","traceID":"123e456e789e0123","rule_name":"simple_trace"}`

var extractBenchCases = []struct {
	json          []byte
	extractFields []cfg.FieldSelector
}{
	{
		json:          []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(10))),
		extractFields: []cfg.FieldSelector{"level"},
	},
	{
		json:          []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(100))),
		extractFields: []cfg.FieldSelector{"level"},
	},
	{
		json:          []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(500))),
		extractFields: []cfg.FieldSelector{"level"},
	},
	{
		json:          []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(1000))),
		extractFields: []cfg.FieldSelector{"level"},
	},
	{
		json:          []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(10000))),
		extractFields: []cfg.FieldSelector{"level"},
	},
	{
		json: []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(10))),
		extractFields: []cfg.FieldSelector{
			"field3",
			"field4",
			"field5",
			"field6",
			"field7",
		},
	},
	{
		json: []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(100))),
		extractFields: []cfg.FieldSelector{
			"field30",
			"field40",
			"field50",
			"field60",
			"field70",
		},
	},
	{
		json: []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(500))),
		extractFields: []cfg.FieldSelector{
			"field1",
			"field100",
			"field200",
			"field300",
			"field400",
		},
	},
	{
		json: []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(1000))),
		extractFields: []cfg.FieldSelector{
			"field300",
			"field400",
			"field500",
			"field600",
			"field700",
		},
	},
	{
		json: []byte(fmt.Sprintf(extractBenchJsonFormat, genBenchFields(10000))),
		extractFields: []cfg.FieldSelector{
			"field3000",
			"field4000",
			"field5000",
			"field6000",
			"field7000",
		},
	},
}

func BenchmarkExtract(b *testing.B) {
	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_len-%d_ext_fields_count-%d", len(benchCase.json), len(benchCase.extractFields))
		extractFields := newPathTree()
		for _, f := range benchCase.extractFields {
			extractFields.add(cfg.ParseFieldSelector(string(f)))
		}

		b.Run(name, func(b *testing.B) {
			d := &jx.Decoder{}
			for i := 0; i < b.N; i++ {
				d.ResetBytes(benchCase.json)
				// remove allocs for adding new fields to root by passing `skipAddField` flag for correct benching
				extract(nil, d, extractFields.root.children, true)
			}
		})
	}
}

func BenchmarkInsaneDecodeDig(b *testing.B) {
	for _, benchCase := range extractBenchCases {
		name := fmt.Sprintf("json_len-%d_ext_fields_count-%d", len(benchCase.json), len(benchCase.extractFields))
		extractFields := make([][]string, 0, len(benchCase.extractFields))
		for _, f := range benchCase.extractFields {
			extractFields = append(extractFields, cfg.ParseFieldSelector(string(f)))
		}

		b.Run(name, func(b *testing.B) {
			node := insaneJSON.Spawn()
			for i := 0; i < b.N; i++ {
				_ = node.DecodeBytes(benchCase.json)
				for _, f := range extractFields {
					_ = node.Dig(f...)
				}
			}
			insaneJSON.Release(node)
		})
	}
}
