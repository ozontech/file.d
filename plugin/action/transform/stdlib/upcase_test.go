package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpcase(t *testing.T) {
	t.Parallel()

	call := func(v string) string {
		got, err := upcase{}.Call(map[string]core.Value{
			"value": core.StringValue{V: v},
		})
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	tests := []struct {
		name string
		in   string
		want string
	}{
		{"lowercase", "info", "INFO"},
		{"mixed_case", "WaRnInG", "WARNING"},
		{"already_upper", "ERROR", "ERROR"},
		{"empty", "", ""},
		{"digits_and_symbols_untouched", "err-404_x!", "ERR-404_X!"},
		{"unicode", "приложение", "ПРИЛОЖЕНИЕ"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, call(tc.in))
		})
	}

	t.Run("non_string_arg_rejected", func(t *testing.T) {
		t.Parallel()

		_, err := resolveArgs(t, upcase{},
			[]core.Value{core.IntegerValue{V: 42}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected string")
		assert.Contains(t, err.Error(), "got integer")
	})

	t.Run("missing_arg_rejected", func(t *testing.T) {
		t.Parallel()

		_, err := resolveArgs(t, upcase{}, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required argument")
	})
}
