package stdlib

import (
	"math"
	"runtime"
	"strings"
	"testing"
	"unsafe"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestSlice(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		input string
		start int64
		end   core.Value
		want  string
	}
	tests := []testCase{
		{
			name:  "first characters",
			input: "some looooooooooooong data",
			end:   core.IntegerValue{V: 10},
			want:  "some loooo",
		},
		{
			name:  "last characters with default end",
			input: "some looooooooooooong data",
			start: -5,
			want:  " data",
		},
		{
			name:  "explicit null end",
			input: "Supercalifragilistic",
			start: 9,
			end:   core.NullValue{},
			want:  "fragilistic",
		},
		{
			name:  "middle",
			input: "Supercalifragilistic",
			start: 5,
			end:   core.IntegerValue{V: 13},
			want:  "califrag",
		},
		{
			name:  "negative bounds",
			input: "abcdef",
			start: -4,
			end:   core.IntegerValue{V: -1},
			want:  "cde",
		},
		{
			name:  "full range",
			input: "whole",
			want:  "whole",
		},
		{
			name:  "end beyond input",
			input: "short",
			end:   core.IntegerValue{V: 100},
			want:  "short",
		},
		{
			name:  "start before input",
			input: "short",
			start: -100,
			want:  "short",
		},
		{
			name:  "start beyond input",
			input: "short",
			start: 100,
		},
		{
			name:  "end before input",
			input: "short",
			end:   core.IntegerValue{V: -100},
		},
		{
			name:  "reversed range",
			input: "short",
			start: 3,
			end:   core.IntegerValue{V: 1},
		},
		{
			name:  "empty range",
			input: "short",
			start: 2,
			end:   core.IntegerValue{V: 2},
		},
		{
			name: "empty input",
			end:  core.IntegerValue{V: 5},
		},
		{
			name:  "Unicode prefix",
			input: "привет мир",
			end:   core.IntegerValue{V: 6},
			want:  "привет",
		},
		{
			name:  "Unicode suffix",
			input: "привет мир",
			start: -3,
			want:  "мир",
		},
		{
			name:  "mixed rune widths",
			input: "aé界🙂z",
			start: 1,
			end:   core.IntegerValue{V: -1},
			want:  "é界🙂",
		},
		{
			name:  "negative start and positive Unicode end",
			input: "aé界🙂z",
			start: -4,
			end:   core.IntegerValue{V: 4},
			want:  "é界🙂",
		},
		{
			name:  "mixed bounds outside Unicode input",
			input: "界🙂",
			start: -3,
			end:   core.IntegerValue{V: 3},
			want:  "界🙂",
		},
		{
			name:  "NUL is one character",
			input: "a\x00界z",
			start: 1,
			end:   core.IntegerValue{V: 3},
			want:  "\x00界",
		},
		{
			name:  "adjacent invalid bytes",
			input: "a\xff\xfez",
			start: 1,
			end:   core.IntegerValue{V: 3},
			want:  "\ufffd\ufffd",
		},
		{
			name:  "truncated encoding",
			input: "\xe2\x82",
			want:  "\ufffd\ufffd",
		},
		{
			name:  "overlong encoding",
			input: "\xf0\x80\x80\x80",
			want:  "\ufffd\ufffd\ufffd\ufffd",
		},
		{
			name:  "literal replacement rune beside invalid byte",
			input: "a\ufffd\xffb",
			start: 1,
			end:   core.IntegerValue{V: 3},
			want:  "\ufffd\ufffd",
		},
		{
			name:  "invalid bytes before selected range",
			input: "\xff\xfe界z",
			start: 2,
			want:  "界z",
		},
		{
			name:  "invalid bytes after selected range",
			input: "界\xff\xfe",
			end:   core.IntegerValue{V: 1},
			want:  "界",
		},
		{
			name:  "negative index counts each invalid byte",
			input: "a\xff\xfez",
			start: -3,
			end:   core.IntegerValue{V: -1},
			want:  "\ufffd\ufffd",
		},
		{
			name:  "minimum start and maximum end",
			input: "short",
			start: math.MinInt64,
			end:   core.IntegerValue{V: math.MaxInt64},
			want:  sliceRuneReference("short", math.MinInt64, math.MaxInt64, false),
		},
		{
			name:  "maximum start",
			input: "short",
			start: math.MaxInt64,
			want:  sliceRuneReference("short", math.MaxInt64, 0, true),
		},
		{
			name:  "minimum end",
			input: "short",
			end:   core.IntegerValue{V: math.MinInt64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, err := compile(slice{})
			require.NoError(t, err)
			values := []core.Value{str(tt.input), core.IntegerValue{V: tt.start}, core.NullValue{}}
			args := []core.Argument{
				{
					Value: &core.ConstExpr{V: values[0]},
				},
				{
					Value: &core.ConstExpr{V: values[1]},
				},
			}
			var named map[string]core.Value
			if tt.end != nil {
				values[2] = tt.end
				named = map[string]core.Value{"end": tt.end}
				args = append(args, core.Argument{
					Name:  "end",
					Value: &core.ConstExpr{V: tt.end},
				})
			}
			expr := &core.CallExpr{
				Node: core.NewNode(preparedTestPosition{}),
				Name: fn.Name(),
				Args: args,
				Plan: fn.PrepareCall(args),
			}
			require.NotNil(t, expr.Plan)
			state := &core.CallState{}
			t.Cleanup(state.Reset)
			ctx := &preparedStateContext{state: state}
			type callMode struct {
				name string
				call func() (core.Value, error)
			}
			modes := []callMode{
				{
					name: "general",
					call: func() (core.Value, error) {
						return callFn(slice{}, values[:2], named)
					},
				},
				{
					name: "indexed",
					call: func() (core.Value, error) {
						return slice{}.CallIndexed(values)
					},
				},
				{
					name: "prepared without state",
					call: func() (core.Value, error) {
						return expr.Eval(nil)
					},
				},
				{
					name: "prepared reused context",
					call: func() (core.Value, error) {
						return expr.Eval(ctx)
					},
				},
			}
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					for range 2 {
						got, callErr := mode.call()
						require.NoError(t, callErr)
						require.Equal(t, str(tt.want), got)
					}
				})
			}
		})
	}
}

func TestSliceDoesNotRetainLargeInput(t *testing.T) {
	t.Parallel()

	input := strings.Repeat("ab界🙂", 32*1024)
	type testCase struct {
		name  string
		start int64
		end   core.Value
		want  string
	}
	tests := []testCase{
		{
			name: "prefix",
			end:  core.IntegerValue{V: 3},
			want: "ab界",
		},
		{
			name:  "middle",
			start: 64 * 1024,
			end:   core.IntegerValue{V: 64*1024 + 3},
			want:  "ab界",
		},
		{
			name:  "suffix",
			start: -3,
			end:   core.NullValue{},
			want:  "b界🙂",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			args := []core.Value{str(input), core.IntegerValue{V: tt.start}, tt.end}
			got, err := slice{}.CallIndexed(args)
			require.NoError(t, err)
			result := got.(core.StringValue).V
			require.Equal(t, tt.want, result)

			// Comparing addresses detects retained backing storage without relying
			// on GC timing or global heap statistics. No memory is read or mutated.
			inputAddress := uintptr(unsafe.Pointer(unsafe.StringData(input)))
			resultAddress := uintptr(unsafe.Pointer(unsafe.StringData(result)))
			require.False(t, resultAddress >= inputAddress && resultAddress < inputAddress+uintptr(len(input)),
				"a short result must not retain the input's backing storage")
			runtime.KeepAlive(input)
			runtime.KeepAlive(result)
		})
	}
}

func FuzzSliceMatchesRuneConversion(f *testing.F) {
	type seed struct {
		input string
		start int64
		end   int64
		null  bool
	}
	seeds := []seed{
		{
			input: "",
			null:  true,
		},
		{
			input: "ASCII",
			start: 1,
			end:   4,
		},
		{
			input: "aé界🙂z",
			start: -4,
			end:   -1,
		},
		{
			input: "a\x00界z",
			start: 1,
			end:   3,
		},
		{
			input: "a\xff\xfez",
			start: -3,
			end:   -1,
		},
		{
			input: "\xe2\x82",
			null:  true,
		},
		{
			input: "\xf0\x80\x80\x80",
			end:   3,
		},
		{
			input: "a\ufffd\xffb",
			start: 1,
			end:   3,
		},
		{
			input: "\xff\xfe界z",
			start: 2,
			null:  true,
		},
		{
			input: "界\xff\xfe",
			end:   1,
		},
		{
			input: "short",
			start: math.MinInt64,
			end:   math.MaxInt64,
		},
		{
			input: "short",
			start: math.MaxInt64,
			end:   math.MinInt64,
		},
		{
			input: "short",
			start: 3,
			end:   1,
		},
	}
	for _, seed := range seeds {
		f.Add(seed.input, seed.start, seed.end, seed.null)
	}
	f.Fuzz(func(t *testing.T, input string, start, end int64, null bool) {
		var endValue core.Value = core.IntegerValue{V: end}
		if null {
			endValue = core.NullValue{}
		}
		args := []core.Value{str(input), core.IntegerValue{V: start}, endValue}

		got, err := slice{}.CallIndexed(args)

		require.NoError(t, err)
		require.Equal(t, str(sliceRuneReference(input, start, end, null)), got)
	})
}

// sliceRuneReference preserves the original conversion and clamping semantics.
func sliceRuneReference(input string, start, end int64, null bool) string {
	runes := []rune(input)
	bounds := [2]int{int(start), int(end)}
	if null {
		bounds[1] = len(runes)
	}
	for i, bound := range bounds {
		if bound < 0 {
			bound += len(runes)
		}
		bounds[i] = max(0, min(bound, len(runes)))
	}
	if bounds[1] <= bounds[0] {
		return ""
	}
	return string(runes[bounds[0]:bounds[1]])
}
