package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func trimCaller(t *testing.T, fn Function) func(value, cutset string) string {
	t.Helper()

	return func(value, cutset string) string {
		got, err := callFn(fn, []core.Value{str(value), str(cutset)}, nil)
		require.NoError(t, err)
		return got.(core.StringValue).V
	}
}

func TestTrim(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trim{})

	assert.Equal(t, "data", call("  data  ", " "))
	assert.Equal(t, "data", call("xxdataxx", "x"))
	assert.Equal(t, "", call("aaa", "a"), "everything trimmed away")
	assert.Equal(t, "", call("", " "), "empty input")
	assert.Equal(t, "data", call("data", ""), "empty cutset changes nothing")
}

func TestTrimLeft(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trimLeft{})

	assert.Equal(t, "data  ", call("  data  ", " "))
	assert.Equal(t, "compaction", call("] compaction", "] "),
		"cutset is a set of characters, so both ] and space are stripped")
	assert.Equal(t, "data", call("data", "x"), "nothing to trim")
}

func TestTrimRight(t *testing.T) {
	t.Parallel()
	call := trimCaller(t, trimRight{})

	// modify README: ${message|trim("right","\n")}
	assert.Equal(t,
		`{"service":"service-test-1","took":"200ms"}`,
		call(`{"service":"service-test-1","took":"200ms"}`+"\n", "\n"))

	assert.Equal(t, "  data", call("  data  ", " "))
	assert.Equal(t, "took: 200", call("took: 200ms", "ms"),
		"cutset is a set of characters, not a suffix")
}
