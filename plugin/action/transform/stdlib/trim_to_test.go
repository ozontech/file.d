package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func trimToCaller(t *testing.T, fn Function) func(value, cutset string, named map[string]core.Value) string {
	t.Helper()

	return func(value, cutset string, named map[string]core.Value) string {
		got, err := callFn(fn, []core.Value{str(value), str(cutset)}, named)
		require.NoError(t, err)
		return got.(core.StringValue).V
	}
}

func TestTrimTo(t *testing.T) {
	t.Parallel()
	call := trimToCaller(t, trimTo{})

	assert.Equal(t, "[data]", call("noise [data] noise", "[data]", nil))
	assert.Equal(t, "|a|b|", call("noise |a|b| noise", "|", nil),
		"the left side takes the first occurrence and the right side the last one")
	assert.Equal(t, "END message END", call("leading END message END trailing", "END", nil),
		"a multi-byte delimiter spans the outermost pair")
	assert.Equal(t, "unchanged", call("unchanged", "@", nil), "delimiter not found")
	assert.Equal(t, "", call("", "@", nil), "empty input")
	assert.Equal(t, "data", call("data", "", nil), "empty cutset changes nothing")

	assert.Equal(t, "|a|b|", call("noise |a|b| noise", "|",
		map[string]core.Value{"include_cutset": core.BoolValue{V: false}},
	))
	assert.Equal(t, "a|b", call("noise |a|b| noise", "|",
		map[string]core.Value{"include_cutset": core.BoolValue{V: true}},
	))
}

func TestTrimToLeft(t *testing.T) {
	t.Parallel()
	call := trimToCaller(t, trimToLeft{})

	assert.Equal(t, "{a} tail", call("head {a} tail", "{", nil), "the delimiter is kept")
	assert.Equal(t, "-> b -> c", call("a -> b -> c", "->", nil),
		"a multi-byte delimiter is matched as a substring, not as a set of characters")
	assert.Equal(t, "unchanged", call("unchanged", "{", nil), "delimiter not found")
	assert.Equal(t, "data", call("data", "", nil), "empty cutset changes nothing")

	assert.Equal(t, "-> b -> c", call("a -> b -> c", "->",
		map[string]core.Value{"include_cutset": core.BoolValue{V: false}},
	))
	assert.Equal(t, " b -> c", call("a -> b -> c", "->",
		map[string]core.Value{"include_cutset": core.BoolValue{V: true}},
	))
}

func TestTrimToRight(t *testing.T) {
	t.Parallel()
	call := trimToCaller(t, trimToRight{})

	assert.Equal(t, "head {a}", call("head {a} tail", "}", nil), "the delimiter is kept")
	assert.Equal(t, "a -> b ->", call("a -> b -> c", "->", nil),
		"the last occurrence wins and the whole delimiter survives")
	assert.Equal(t, "ключ=значение", call("ключ=значение и хвост", "значение", nil),
		"a multi-byte delimiter is never cut in half")
	assert.Equal(t, "hello界", call("hello界 trailing", "界", nil), "a unicode delimiter is kept whole")
	assert.Equal(t, "unchanged", call("unchanged", "}", nil), "delimiter not found")
	assert.Equal(t, "data", call("data", "", nil), "empty cutset changes nothing")
	assert.Equal(t, "", call("", "", nil), "empty input and empty cutset")

	assert.Equal(t, "a -> b ->", call("a -> b -> c", "->",
		map[string]core.Value{"include_cutset": core.BoolValue{V: false}},
	))
	assert.Equal(t, "a -> b ", call("a -> b -> c", "->",
		map[string]core.Value{"include_cutset": core.BoolValue{V: true}},
	))
}

// the modify plugin's README example, which is the reason this family exists:
// `${message|trim_to("left","{")|trim_to("right","}")}`
func TestTrimToExtractsJSONObject(t *testing.T) {
	t.Parallel()

	left := trimCaller(t, trimToLeft{})
	right := trimCaller(t, trimToRight{})

	const line = `some data {"service":"service-test-1","took":"200ms"} some data`

	assert.Equal(t,
		`{"service":"service-test-1","took":"200ms"}`,
		right(left(line, "{"), "}"))

	// the nested case is what the substring family cannot do: between() stops at
	// the first closing brace, trim_to spans the outermost pair
	assert.Equal(t,
		`{"a":{"b":1}}`,
		right(left(`some data {"a":{"b":1}} some data`, "{"), "}"))
}
