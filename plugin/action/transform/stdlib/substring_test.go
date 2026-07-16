package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func str(v string) core.Value { return core.StringValue{V: v} }

func TestAfter(t *testing.T) {
	t.Parallel()

	call := func(value, sep string) string {
		got, err := after{}.Call(map[string]core.Value{
			"value":     str(value),
			"separator": str(sep),
		})
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	assert.Equal(t, "world", call("hello - world", " - "))
	assert.Equal(t, "b - c", call("a - b - c", " - "), "splits on the FIRST occurrence")
	assert.Equal(t, "no separator here", call("no separator here", " - "), "not found: unchanged")
	assert.Equal(t, "", call("ends with - ", " - "))
}

func TestBefore(t *testing.T) {
	t.Parallel()

	call := func(value, sep string) string {
		got, err := before{}.Call(map[string]core.Value{
			"value":     str(value),
			"separator": str(sep),
		})
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	assert.Equal(t, "hello", call("hello - world", " - "))
	assert.Equal(t, "a", call("a - b - c", " - "), "splits on the FIRST occurrence")
	assert.Equal(t, "no separator here", call("no separator here", " - "), "not found: unchanged")
	assert.Equal(t, "", call(" - starts with", " - "))
}

func TestBetween(t *testing.T) {
	t.Parallel()

	call := func(value, open, closing string) string {
		got, err := between{}.Call(map[string]core.Value{
			"value": str(value),
			"open":  str(open),
			"close": str(closing),
		})
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	assert.Equal(t, "shard 4:comp", call("INFO [shard 4:comp] compaction", "[", "]"))
	assert.Equal(t, "first", call("[first] and [second]", "[", "]"), "first pair wins")
	assert.Equal(t, "", call("empty []", "[", "]"))
	assert.Equal(t, "no brackets", call("no brackets", "[", "]"), "open not found: unchanged")
	assert.Equal(t, "unclosed [rest", call("unclosed [rest", "[", "]"), "close not found: unchanged")
	assert.Equal(t, "mid", call("a <<mid>> b", "<<", ">>"), "multi-char delimiters")
}
