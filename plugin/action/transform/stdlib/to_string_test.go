package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	t.Parallel()

	call := func(v core.Value) string {
		got, err := toString{}.Call(map[string]core.Value{"value": v})
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	tests := []struct {
		name string
		in   core.Value
		want string
	}{
		{"string_passthrough", core.StringValue{V: "hello"}, "hello"},
		{"integer", core.IntegerValue{V: 42}, "42"},
		{"float", core.FloatValue{V: 1.5}, "1.5"},
		{"bool_true", core.BoolValue{V: true}, "true"},
		{"bool_false", core.BoolValue{V: false}, "false"},
		{"null_is_empty", core.NullValue{}, ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, call(tc.in))
		})
	}
}
