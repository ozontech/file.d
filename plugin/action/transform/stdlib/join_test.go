package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	t.Parallel()

	call := func(elements []core.Value, separator string) (core.Value, error) {
		return callFn(join{}, []core.Value{core.ArrayValue{V: elements}, str(separator)}, nil)
	}
	mustCall := func(t *testing.T, elements []core.Value, separator string) string {
		t.Helper()
		got, err := call(elements, separator)
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	t.Run("separator", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "re1,re2", mustCall(t, []core.Value{str("re1"), str("re2")}, ","))
		assert.Equal(t, "re1re2", mustCall(t, []core.Value{str("re1"), str("re2")}, ""))
	})

	t.Run("edge_cases", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", mustCall(t, nil, ","), "empty array")
		assert.Equal(t, "only", mustCall(t, []core.Value{str("only")}, ","), "no separator for one element")
		assert.Equal(t, ",", mustCall(t, []core.Value{str(""), str("")}, ","), "empty elements are kept")
	})

	t.Run("rejects_non_strings", func(t *testing.T) {
		t.Parallel()
		_, err := call([]core.Value{str("a"), core.IntegerValue{V: 1}}, ",")
		require.ErrorContains(t, err, "element 1: expected string, got integer")
	})
}
