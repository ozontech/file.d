package decoder

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

func TestDecodeCSV(t *testing.T) {
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          []string
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name:  "CRLF",
			input: `a,b,c` + "\r" + "\n",
			want:  CSVRow{"a", "b", "c"},
		},
		{
			name:  "default_delimiter",
			input: `a,"bb""b","c,c,c"` + "\n",
			want:  CSVRow{"a", "bb\"b", "c,c,c"},
		},
		{
			name:  "custom_delimiter",
			input: `a	b	"c"` + "\n",
			params: map[string]any{
				delimiterParam: "\t",
			},
			want: CSVRow{"a", "b", "c"},
		},
		{
			name:  "invalid_column_names",
			input: "",
			params: map[string]any{
				columnNamesParam: "name",
			},
			wantCreateErr: true,
		},
		{
			name:  "invalid_delimiter_1",
			input: "",
			params: map[string]any{
				delimiterParam: ",,",
			},
			wantCreateErr: true,
		},
		{
			name:  "invalid_delimiter_2",
			input: "",
			params: map[string]any{
				delimiterParam: "\n",
			},
			wantCreateErr: true,
		},
		{
			name:  "wrong_number_of_fields",
			input: "a,b,c,d" + "\n",
			params: map[string]any{
				columnNamesParam: []any{"column_a", "column_b", "column_c"},
			},
			wantDecodeErr: true,
		},
		{
			name:          "missing_quote_quoted_field",
			input:         `a,"b,c,d` + "\n",
			wantDecodeErr: true,
		},
		{
			name:          "missing_quote_non_quoted_field",
			input:         `a,b",c,d` + "\n",
			wantDecodeErr: true,
		},
		{
			name:          "non_escaped_quote",
			input:         `a,b,"cc"c",d` + "\n",
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewCSVDecoder(tt.params)
			assert.Equal(t, tt.wantCreateErr, err != nil)
			if tt.wantCreateErr {
				return
			}

			row, err := d.Decode([]byte(tt.input))
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}

			for i, v := range row.(CSVRow) {
				assert.Equal(t, tt.want[i], v)
			}
		})
	}
}

func TestDecodeToJsonCSV(t *testing.T) {
	tests := []struct {
		name string

		input  string
		params map[string]any

		want          string
		wantDecodeErr bool
	}{
		{
			name:  "empty_params",
			input: `a,b,c` + "\n",
			want:  `{"0":"a","1":"b","2":"c"}`,
		},
		{
			name:  "custom_column_names",
			input: `"a","""b""","c"` + "\n",
			params: map[string]any{
				columnNamesParam: []any{"service", "version", "info"},
			},
			want: `{"service":"a","version":"\"b\"","info":"c"}`,
		},
		{
			name:  "custom_prefix",
			input: `"a";"""b""";"c"` + "\n",
			params: map[string]any{
				prefixParam:    "csv_",
				delimiterParam: ";",
			},
			want: `{"csv_0":"a","csv_1":"\"b\"","csv_2":"c"}`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, err := NewCSVDecoder(tt.params)
			assert.Nil(t, err)

			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			err = d.DecodeToJson(root, []byte(tt.input))
			assert.Equal(t, tt.wantDecodeErr, err != nil)
			if tt.wantDecodeErr {
				return
			}

			assert.Equal(t, tt.want, root.EncodeToString())
		})
	}
}
