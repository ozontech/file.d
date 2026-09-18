package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	t.Parallel()

	call := func(value string, start int64, end ...int64) string {
		named := map[string]core.Value{}
		if len(end) > 0 {
			named["end"] = core.IntegerValue{V: end[0]}
		}
		got, err := callFn(slice{}, []core.Value{str(value), core.IntegerValue{V: start}}, named)
		require.NoError(t, err)
		return got.(core.StringValue).V
	}

	t.Run("modify_cut_equivalents", func(t *testing.T) {
		t.Parallel()
		// modify README: ${message|cut("first",10)} and ${message|cut("last",5)}
		assert.Equal(t, "some loooo", call("some looooooooooooong data", 0, 10))
		assert.Equal(t, " data", call("some looooooooooooong data", -5))
	})

	t.Run("positions", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "califrag", call("Supercalifragilistic", 5, 13))
		assert.Equal(t, "listic", call("Supercalifragilistic", -6))
		assert.Equal(t, "fragilistic", call("Supercalifragilistic", 9))
		assert.Equal(t, "Supercalifragilistic", call("Supercalifragilistic", 0))
	})

	t.Run("out_of_range_is_clamped", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "short", call("short", 0, 100), "end past the string returns what is there")
		assert.Equal(t, "short", call("short", -100), "start before the string starts at 0")
		assert.Equal(t, "", call("short", 100), "start past the string yields nothing")
		assert.Equal(t, "", call("short", 3, 1), "end before start yields nothing")
		assert.Equal(t, "", call("short", 2, 2), "empty range")
		assert.Equal(t, "", call("", 0, 5), "empty input")
	})

	t.Run("counts_characters_not_bytes", func(t *testing.T) {
		t.Parallel()
		// The modify plugin's cut counts bytes and would split these runes.
		assert.Equal(t, "привет", call("привет мир", 0, 6))
		assert.Equal(t, "мир", call("привет мир", -3))
	})
}
