package decoder

import (
	"testing"

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
			name:  "default_delimiter",
			input: `abc,"zx""c","d,e,f"` + "\n",
			want:  CSVRow{"abc", "zx\"c", "d,e,f"},
		},
		{
			name:  "custom_delimiter",
			input: `qwe	asd	"rty"` + "\n",
			params: map[string]any{
				delimiterParam: "\t",
			},
			want: CSVRow{"qwe", "asd", "rty"},
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
			name:  "wrong_number_of_fields",
			input: "qwe,asd,zxc,rty" + "\n",
			params: map[string]any{
				columnNamesParam: []any{"a", "b", "c"},
			},
			wantDecodeErr: true,
		},
		{
			name:          "missing_quote_quoted_field",
			input:         `qwe,"asd,zxc,rty` + "\n",
			wantDecodeErr: true,
		},
		{
			name:          "missing_quote_non_quoted_field",
			input:         `qwe,asd",zxc,rty` + "\n",
			wantDecodeErr: true,
		},
		{
			name:          "non_escaped_quote",
			input:         `qwe,asd,"zx"c",rty` + "\n",
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
