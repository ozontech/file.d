package encoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewRawEncoder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    *RawEncoderParams
		wantField string
	}{
		{
			name:      "custom field",
			params:    &RawEncoderParams{Field: "data"},
			wantField: "data",
		},
		{
			name:      "empty field falls back to message",
			params:    &RawEncoderParams{Field: ""},
			wantField: "message",
		},
		{
			name:      "nested field path",
			params:    &RawEncoderParams{Field: "log.message"},
			wantField: "log.message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc := newRawEncoder(tt.params)
			require.NotNil(t, enc)
			assert.Equal(t, tt.wantField, enc.field)
		})
	}
}

func TestRawEncode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		field    string
		input    string
		expected string
	}{
		{
			name:     "string field returns raw value without quotes",
			field:    "message",
			input:    `{"message":"hello world"}`,
			expected: "hello world",
		},
		{
			name:     "string field with special characters keeps them raw",
			field:    "message",
			input:    `{"message":"line1\nline2"}`,
			expected: "line1\nline2",
		},
		{
			name:     "missing field returns empty",
			field:    "message",
			input:    `{"other":"value"}`,
			expected: "",
		},
		{
			name:     "number field returns encoded representation",
			field:    "code",
			input:    `{"code":200}`,
			expected: "200",
		},
		{
			name:     "bool field returns encoded representation",
			field:    "ok",
			input:    `{"ok":true}`,
			expected: "true",
		},
		{
			name:     "object field returns encoded JSON",
			field:    "data",
			input:    `{"data":{"a":1}}`,
			expected: `{"a":1}`,
		},
		{
			name:     "array field returns encoded JSON",
			field:    "items",
			input:    `{"items":[1,2,3]}`,
			expected: `[1,2,3]`,
		},
		{
			name:     "null field returns encoded null",
			field:    "value",
			input:    `{"value":null}`,
			expected: "null",
		},
		{
			name:     "empty string field returns empty",
			field:    "message",
			input:    `{"message":""}`,
			expected: "",
		},
		{
			name:     "unescape string field",
			field:    "message",
			input:    `{"message":"{\"log\":\"[INFO] some event\"}","field_a":"AAAA","field_b":"BBBB"}`,
			expected: `{"log":"[INFO] some event"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			enc := newRawEncoder(&RawEncoderParams{Field: tt.field})
			event := newTestEvent(t, tt.input)

			out := enc.Encode(event, nil)
			assert.Equal(t, tt.expected, string(out))
		})
	}
}
