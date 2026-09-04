package encoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONEncode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple object",
			input:    `{"message":"hello"}`,
			expected: `{"message":"hello"}`,
		},
		{
			name:     "nested object",
			input:    `{"level":"info","data":{"a":1,"b":[1,2,3]}}`,
			expected: `{"level":"info","data":{"a":1,"b":[1,2,3]}}`,
		},
		{
			name:     "array root",
			input:    `[1,2,3]`,
			expected: `[1,2,3]`,
		},
		{
			name:     "scalar string root",
			input:    `"just a string"`,
			expected: `"just a string"`,
		},
		{
			name:     "number root",
			input:    `42`,
			expected: `42`,
		},
		{
			name:     "empty object",
			input:    `{}`,
			expected: `{}`,
		},
		{
			name:     "special characters escaped",
			input:    `{"msg":"line1\nline2\t\"quoted\""}`,
			expected: `{"msg":"line1\nline2\t\"quoted\""}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc := newJSONEncoder(&JSONEncoderParams{})
			event := newTestEvent(t, tt.input)

			out, err := enc.Encode(event, nil)
			require.NoError(t, err)
			assert.JSONEq(t, tt.expected, string(out))
		})
	}
}
