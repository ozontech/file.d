package stdlib

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTrimTo(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trimTo{})

	assert.Equal(t, "[data]", call("noise [data] noise", "[data]"))
	assert.Equal(t, "|a|b|", call("noise |a|b| noise", "|"),
		"the left side takes the first occurrence and the right side the last one")
	assert.Equal(t, "END message END", call("leading END message END trailing", "END"),
		"a multi-byte delimiter spans the outermost pair")
	assert.Equal(t, "unchanged", call("unchanged", "@"), "delimiter not found")
	assert.Equal(t, "", call("", "@"), "empty input")
	assert.Equal(t, "data", call("data", ""), "empty cutset changes nothing")
}

func TestTrimToLeft(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trimToLeft{})

	assert.Equal(t, "{a} tail", call("head {a} tail", "{"), "the delimiter is kept")
	assert.Equal(t, "-> b -> c", call("a -> b -> c", "->"),
		"a multi-byte delimiter is matched as a substring, not as a set of characters")
	assert.Equal(t, "unchanged", call("unchanged", "{"), "delimiter not found")
	assert.Equal(t, "data", call("data", ""), "empty cutset changes nothing")
}

func TestTrimToRight(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trimToRight{})

	assert.Equal(t, "head {a}", call("head {a} tail", "}"), "the delimiter is kept")
	assert.Equal(t, "a -> b ->", call("a -> b -> c", "->"),
		"the last occurrence wins and the whole delimiter survives")
	assert.Equal(t, "ключ=значение", call("ключ=значение и хвост", "значение"),
		"a multi-byte delimiter is never cut in half")
	assert.Equal(t, "hello界", call("hello界 trailing", "界"), "a unicode delimiter is kept whole")
	assert.Equal(t, "unchanged", call("unchanged", "}"), "delimiter not found")
	assert.Equal(t, "data", call("data", ""), "empty cutset changes nothing")
	assert.Equal(t, "", call("", ""), "empty input and empty cutset")
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
