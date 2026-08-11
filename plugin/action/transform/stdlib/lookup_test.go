package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func kafkaAPIKeys() core.ObjectValue {
	return core.ObjectValue{V: map[string]core.Value{
		"0": core.StringValue{V: "produce"},
		"1": core.StringValue{V: "fetch"},
		"2": core.StringValue{V: "offsets"},
	}}
}

// callLookup resolves the arguments the way the interpreter does, so defaults
// and kind checks are exercised rather than bypassed.
func callLookup(t *testing.T, positional []core.Value, named map[string]core.Value) (core.Value, error) {
	t.Helper()

	args, err := resolveArgs(t, lookup{}, positional, named)
	if err != nil {
		return nil, err
	}
	return lookup{}.Call(args)
}

func TestLookup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value core.Value
		want  core.Value
	}{
		{
			name:  "string key hits",
			value: core.StringValue{V: "1"},
			want:  core.StringValue{V: "fetch"},
		},
		{
			// JSON writes enumeration codes both ways; both must hit.
			name:  "integer key hits the same entry",
			value: core.IntegerValue{V: 1},
			want:  core.StringValue{V: "fetch"},
		},
		{
			name:  "zero is not confused with a miss",
			value: core.IntegerValue{V: 0},
			want:  core.StringValue{V: "produce"},
		},
		{
			name:  "miss returns the value unchanged",
			value: core.StringValue{V: "77"},
			want:  core.StringValue{V: "77"},
		},
		{
			name:  "missing field stays null",
			value: core.NullValue{},
			want:  core.NullValue{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := callLookup(t, []core.Value{tt.value, kafkaAPIKeys()}, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLookupDefault(t *testing.T) {
	t.Parallel()

	t.Run("replaces a miss", func(t *testing.T) {
		t.Parallel()

		got, err := callLookup(t,
			[]core.Value{core.StringValue{V: "77"}, kafkaAPIKeys()},
			map[string]core.Value{"default": core.StringValue{V: "unknown"}},
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "unknown"}, got)
	})

	t.Run("does not replace a hit", func(t *testing.T) {
		t.Parallel()

		got, err := callLookup(t,
			[]core.Value{core.StringValue{V: "1"}, kafkaAPIKeys()},
			map[string]core.Value{"default": core.StringValue{V: "unknown"}},
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "fetch"}, got)
	})

	// An explicit null must be distinguishable from an omitted default, which
	// is why the omitted case uses a sentinel rather than null.
	t.Run("explicit null differs from omitted", func(t *testing.T) {
		t.Parallel()

		got, err := callLookup(t,
			[]core.Value{core.StringValue{V: "77"}, kafkaAPIKeys()},
			map[string]core.Value{"default": core.NullValue{}},
		)
		require.NoError(t, err)
		assert.Equal(t, core.NullValue{}, got)
	})

	t.Run("a null entry is returned as null", func(t *testing.T) {
		t.Parallel()

		table := core.ObjectValue{V: map[string]core.Value{"a": core.NullValue{}}}
		got, err := callLookup(t, []core.Value{core.StringValue{V: "a"}, table}, nil)
		require.NoError(t, err)
		assert.Equal(t, core.NullValue{}, got)
	})
}

func TestLookupTableValuesOfAnyType(t *testing.T) {
	t.Parallel()

	table := core.ObjectValue{V: map[string]core.Value{
		"a": core.IntegerValue{V: 7},
		"b": core.ArrayValue{V: []core.Value{core.StringValue{V: "x"}}},
	}}

	got, err := callLookup(t, []core.Value{core.StringValue{V: "a"}, table}, nil)
	require.NoError(t, err)
	assert.Equal(t, core.IntegerValue{V: 7}, got)

	got, err = callLookup(t, []core.Value{core.StringValue{V: "b"}, table}, nil)
	require.NoError(t, err)
	assert.Equal(t, core.ArrayValue{V: []core.Value{core.StringValue{V: "x"}}}, got)
}

func TestLookupRejectsBadArguments(t *testing.T) {
	t.Parallel()

	t.Run("table must be an object", func(t *testing.T) {
		t.Parallel()

		_, err := callLookup(t,
			[]core.Value{core.StringValue{V: "1"}, core.StringValue{V: "not a table"}}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected object")
		assert.Contains(t, err.Error(), "got string")
	})

	t.Run("composite values cannot be keys", func(t *testing.T) {
		t.Parallel()

		_, err := callLookup(t,
			[]core.Value{core.ArrayValue{V: nil}, kafkaAPIKeys()}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "got array")
	})

	t.Run("table is required", func(t *testing.T) {
		t.Parallel()

		_, err := callLookup(t, []core.Value{core.StringValue{V: "1"}}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required argument")
	})

	t.Run("unknown named argument", func(t *testing.T) {
		t.Parallel()

		_, err := callLookup(t,
			[]core.Value{core.StringValue{V: "1"}, kafkaAPIKeys()},
			map[string]core.Value{"fallback": core.StringValue{V: "x"}},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown argument")
	})
}

// The table is shared across events, so a lookup must never write to it.
func TestLookupDoesNotMutateTable(t *testing.T) {
	t.Parallel()

	table := kafkaAPIKeys()

	_, err := callLookup(t, []core.Value{core.StringValue{V: "77"}, table}, nil)
	require.NoError(t, err)

	assert.Len(t, table.V, 3)
}
