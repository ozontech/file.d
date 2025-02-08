package decoder

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

func TestJson(t *testing.T) {
	const inputJson = `{"f1":"v12345","f2":{"f2_1":100,"f2_2":{"f2_2_1":true,"f2_2_2":"v123456789"},"f2_3":[1,2,3]},"f3":null}`
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          map[string]string
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name:  "valid_full",
			input: inputJson,
			want: map[string]string{
				"f1":             "v12345",
				"f2.f2_1":        "100",
				"f2.f2_2.f2_2_1": "true",
				"f2.f2_2.f2_2_2": "v123456789",
				"f2.f2_3":        "[1,2,3]",
				"f3":             "null",
			},
		},
		{
			name:  "valid_max_fields_size",
			input: inputJson,
			params: map[string]any{
				jsonMaxFieldsSizeParam: map[string]any{
					"":               json.Number("1"),
					"not_exists":     json.Number("100"),
					"f2.f2_1":        json.Number("1"),
					"f2.f2_2.f2_2_1": json.Number("3"),
					"f1":             json.Number("5"),
					"f2.f2_2.f2_2_2": json.Number("7"),
				},
			},
			want: map[string]string{
				"f1":             "v1234",
				"f2.f2_1":        "100",
				"f2.f2_2.f2_2_1": "true",
				"f2.f2_2.f2_2_2": "v123456",
				"f2.f2_3":        "[1,2,3]",
				"f3":             "null",
			},
		},
		{
			name:  "valid_max_fields_size_single",
			input: inputJson,
			params: map[string]any{
				jsonMaxFieldsSizeParam: map[string]any{
					"f2.f2_2.f2_2_2": json.Number("4"),
				},
			},
			want: map[string]string{
				"f1":             "v12345",
				"f2.f2_1":        "100",
				"f2.f2_2.f2_2_1": "true",
				"f2.f2_2.f2_2_2": "v123",
				"f2.f2_3":        "[1,2,3]",
				"f3":             "null",
			},
		},
		{
			name: "invalid_create_1",
			params: map[string]any{
				jsonMaxFieldsSizeParam: "not_map",
			},
			wantCreateErr: true,
		},
		{
			name: "invalid_create_2",
			params: map[string]any{
				jsonMaxFieldsSizeParam: map[string]any{
					"test": json.Number("not_num"),
				},
			},
			wantCreateErr: true,
		},
		{
			name: "invalid_create_3",
			params: map[string]any{
				jsonMaxFieldsSizeParam: map[string]any{
					"test": json.Number("1.2"),
				},
			},
			wantCreateErr: true,
		},
		{
			name:          "invalid_decode",
			input:         "invalid json",
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewJsonDecoder(tt.params)
			assert.Equal(t, tt.wantCreateErr, err != nil)
			if tt.wantCreateErr {
				return
			}

			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			nodeRaw, err := d.Decode([]byte(tt.input), root)
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}
			node := nodeRaw.(*insaneJSON.Node)

			for path, val := range tt.want {
				gotNode := node.Dig(cfg.ParseFieldSelector(path)...)
				var gotVal string
				if val != "" && val[0] == '[' {
					gotVal = gotNode.EncodeToString()
				} else {
					gotVal = gotNode.AsString()
				}
				assert.Equal(t, val, gotVal)
			}
		})
	}
}

func genBenchFields(count int) string {
	var sb strings.Builder
	for i := 0; i < count; i++ {
		sb.WriteString(fmt.Sprintf(`"field_%d":"vaaaaaaaaaaaaaal_%d",`, i, i))
	}
	return sb.String()
}

func genBenchParams(count, maxLen int) map[string]int {
	m := map[string]int{}
	for i := 0; i < count; i++ {
		m[fmt.Sprintf("field_%d", i)] = maxLen
	}
	return m
}

func BenchmarkCutFieldsBySize(b *testing.B) {
	const benchJsonFormat = `{%s"level":"info","ts":"2024-02-21T08:31:24.621Z","message":"some message"}`

	var benchCases = []struct {
		json   []byte
		params map[string]int
	}{
		{
			json: []byte(fmt.Sprintf(benchJsonFormat, genBenchFields(0))),
			params: map[string]int{
				"message": 7,
			},
		},
		{
			json:   []byte(fmt.Sprintf(benchJsonFormat, genBenchFields(10))),
			params: genBenchParams(9, 3),
		},
		{
			json:   []byte(fmt.Sprintf(benchJsonFormat, genBenchFields(100))),
			params: genBenchParams(98, 5),
		},
		{
			json:   []byte(fmt.Sprintf(benchJsonFormat, genBenchFields(1000))),
			params: genBenchParams(997, 7),
		},
		{
			json:   []byte(fmt.Sprintf(benchJsonFormat, genBenchFields(10000))),
			params: genBenchParams(9996, 9),
		},
	}

	for _, benchCase := range benchCases {
		name := fmt.Sprintf("json_length_%d", len(benchCase.json))

		b.Run(name, func(b *testing.B) {
			d := jsonDecoder{
				params: jsonParams{
					maxFieldsSize: benchCase.params,
				},
				cutPositions: make([]jsonCutPos, 0, len(benchCase.params)),
				mu:           &sync.Mutex{},
			}
			for i := 0; i < b.N; i++ {
				_ = d.cutFieldsBySize(benchCase.json)
			}
		})
	}
}
