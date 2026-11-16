package stdlib

import (
	"maps"
	"regexp"
	"slices"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestRegisteredFunctionsPrepareCalls(t *testing.T) {
	tests := []struct {
		name string
		args []core.Value
		want core.Value
	}{
		{
			name: "after",
			args: []core.Value{str("a-b"), str("-")},
			want: str("b"),
		},
		{
			name: "before",
			args: []core.Value{str("a-b"), str("-")},
			want: str("a"),
		},
		{
			name: "between",
			args: []core.Value{str("[a]"), str("["), str("]")},
			want: str("a"),
		},
		{
			name: "join",
			args: []core.Value{core.ArrayValue{V: []core.Value{str("a"), str("b")}}, str(",")},
			want: str("a,b"),
		},
		{
			name: "lookup",
			args: []core.Value{str("miss"), core.ObjectValue{V: map[string]core.Value{}}},
			want: str("miss"),
		},
		{
			name: "parse_regex",
			args: []core.Value{str("a"), core.RegexValue{V: regexp.MustCompile("(?P<key>a)")}},
			want: core.ObjectValue{V: map[string]core.Value{"key": str("a")}},
		},
		{
			name: "parse_regex_all",
			args: []core.Value{str("a a"), core.RegexValue{V: regexp.MustCompile("a")}},
			want: core.ArrayValue{V: []core.Value{str("a"), str("a")}},
		},
		{
			name: "slice",
			args: []core.Value{str("abc"), core.IntegerValue{V: 1}},
			want: str("bc"),
		},
		{
			name: "to_string",
			args: []core.Value{core.IntegerValue{V: 42}},
			want: str("42"),
		},
		{
			name: "trim",
			args: []core.Value{str("!a!"), str("!")},
			want: str("a"),
		},
		{
			name: "trim_left",
			args: []core.Value{str("!a!"), str("!")},
			want: str("a!"),
		},
		{
			name: "trim_right",
			args: []core.Value{str("!a!"), str("!")},
			want: str("!a"),
		},
		{
			name: "trim_to",
			args: []core.Value{str("x|a|y"), str("|")},
			want: str("|a|"),
		},
		{
			name: "trim_to_left",
			args: []core.Value{str("x|a|y"), str("|")},
			want: str("|a|y"),
		},
		{
			name: "trim_to_right",
			args: []core.Value{str("x|a|y"), str("|")},
			want: str("x|a|"),
		},
		{
			name: "upcase",
			args: []core.Value{str("a")},
			want: str("A"),
		},
	}

	// A newly registered function must add a case and support prepared calls.
	names := make([]string, len(tests))
	for i, tt := range tests {
		names[i] = tt.name
	}
	require.ElementsMatch(t, slices.Collect(maps.Keys(GetRegistry().functions)), names)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, ok := GetRegistry().Get(tt.name)
			require.True(t, ok)
			args := make([]core.Argument, len(tt.args))
			for i, value := range tt.args {
				args[i] = core.Argument{Value: &core.ConstExpr{V: value}}
			}
			resolved, err := fn.Resolve(tt.args, nil)
			require.NoError(t, err)

			general, generalErr := fn.Call(resolved)
			prepared, preparedErr := preparedTestCall(t, fn, args)

			require.NoError(t, generalErr)
			require.NoError(t, preparedErr)
			require.Equal(t, tt.want, general)
			require.Equal(t, general, prepared)
		})
	}
}

func TestPrepareCallWithoutIndexedSupport(t *testing.T) {
	t.Parallel()

	fn, err := compile(&mockFn{name: "general"})
	require.NoError(t, err)
	require.Nil(t, fn.PrepareCall(nil))
}

func TestPreparedCallsPreserveStringBytes(t *testing.T) {
	// Keep raw strings out of JSON: decoding/encoding can replace invalid UTF-8.
	tests := []struct {
		name     string
		function string
		args     []core.Value
		want     core.Value
	}{
		{
			name:     "slice replaces invalid UTF-8",
			function: "slice",
			args:     []core.Value{str("a\xff\x00界"), core.IntegerValue{V: 1}},
			want:     str("\ufffd\x00界"),
		},
		{
			name:     "lookup matches raw bytes",
			function: "lookup",
			args:     []core.Value{str("\xff\x00"), core.ObjectValue{V: map[string]core.Value{"\xff\x00": str("found")}}},
			want:     str("found"),
		},
		{
			name:     "regex returns original matched bytes",
			function: "parse_regex",
			args:     []core.Value{str("\xff\x00界"), core.RegexValue{V: regexp.MustCompile("(?P<value>.+)")}},
			want:     core.ObjectValue{V: map[string]core.Value{"value": str("\xff\x00界")}},
		},
		{
			name:     "regex_all matches invalid rune and NUL",
			function: "parse_regex_all",
			args:     []core.Value{str("\xff\x00界"), core.RegexValue{V: regexp.MustCompile(".")}},
			want:     core.ArrayValue{V: []core.Value{str("\xff"), str("\x00"), str("界")}},
		},
		{
			name:     "join preserves invalid UTF-8 and NUL",
			function: "join",
			args:     []core.Value{core.ArrayValue{V: []core.Value{str("\xff"), str("界")}}, str("\x00")},
			want:     str("\xff\x00界"),
		},
		{
			name:     "to_string preserves invalid UTF-8 and NUL",
			function: "to_string",
			args:     []core.Value{str("\xff\x00界")},
			want:     str("\xff\x00界"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, ok := GetRegistry().Get(tt.function)
			require.True(t, ok)
			args := make([]core.Argument, len(tt.args))
			for i, value := range tt.args {
				args[i] = core.Argument{Value: &core.ConstExpr{V: value}}
			}
			resolved, err := fn.Resolve(tt.args, nil)
			require.NoError(t, err)

			general, generalErr := fn.Call(resolved)
			prepared, preparedErr := preparedTestCall(t, fn, args)

			require.NoError(t, generalErr)
			require.NoError(t, preparedErr)
			require.Equal(t, tt.want, general)
			require.Equal(t, general, prepared)
		})
	}
}
