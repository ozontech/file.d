package stdlib

import (
	"regexp"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func callFindAll(t *testing.T, value, pattern string, named map[string]core.Value) []string {
	t.Helper()

	got, err := callFn(findAll{}, []core.Value{
		core.StringValue{V: value},
		core.RegexValue{V: regexp.MustCompile(pattern)},
	}, named)
	require.NoError(t, err)

	arr, ok := got.(core.ArrayValue)
	require.True(t, ok, "expected an array, got %s", got.Kind())

	out := make([]string, len(arr.V))
	for i, el := range arr.V {
		out[i] = el.(core.StringValue).V
	}
	return out
}

func TestFindAll(t *testing.T) {
	t.Parallel()

	t.Run("whole_matches", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, []string{"re1", "re2", "re3", "re4"},
			callFindAll(t, "re1 re2 re3 re4", `re\d+`, nil))
	})

	t.Run("capture_group", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, []string{"1", "2"},
			callFindAll(t, "re1 re2", `re(\d+)`, map[string]core.Value{"group": core.IntegerValue{V: 1}}))
	})

	t.Run("limit", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, []string{"re1", "re2"},
			callFindAll(t, "re1 re2 re3 re4", `re\d+`, map[string]core.Value{"limit": core.IntegerValue{V: 2}}),
			"a positive limit caps the number of matches")

		assert.Empty(t, callFindAll(t, "re1 re2", `re\d+`, map[string]core.Value{"limit": core.IntegerValue{V: 0}}),
			"limit 0 extracts nothing, as in the modify plugin")

		assert.Equal(t, []string{"re1", "re2"},
			callFindAll(t, "re1 re2", `re\d+`, map[string]core.Value{"limit": core.IntegerValue{V: -5}}),
			"any negative limit means all matches")
	})

	t.Run("no_match", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, callFindAll(t, "nothing here", `re\d+`, nil))
		assert.Empty(t, callFindAll(t, "", `re\d+`, nil), "empty input")
	})

	t.Run("skips_groups_that_did_not_participate", func(t *testing.T) {
		t.Parallel()
		// Group 1 participates only in the "a" match, not in the "b" one.
		assert.Equal(t, []string{"a"},
			callFindAll(t, "a b", `(a)|(b)`, map[string]core.Value{"group": core.IntegerValue{V: 1}}))
	})

	t.Run("group_out_of_range", func(t *testing.T) {
		t.Parallel()

		call := func(group int64) error {
			_, err := callFn(findAll{}, []core.Value{
				core.StringValue{V: "re1"},
				core.RegexValue{V: regexp.MustCompile(`re(\d+)`)},
			}, map[string]core.Value{"group": core.IntegerValue{V: group}})
			return err
		}

		require.ErrorContains(t, call(2), "out of range")
		require.ErrorContains(t, call(-1), "out of range")
		require.NoError(t, call(1), "the last valid group is accepted")
	})
}
