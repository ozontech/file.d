package stdlib

import (
	"errors"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

type preparedTestPosition struct{}

func (preparedTestPosition) String() string { return "test:1:2" }

func preparedTestCall(t *testing.T, fn *CompiledFunc, args []core.Argument) (core.Value, error) {
	t.Helper()

	plan := fn.PrepareCall(args)
	require.NotNil(t, plan)
	expr := &core.CallExpr{Node: core.NewNode(preparedTestPosition{}), Name: fn.Name(), Args: args, Plan: plan}
	return expr.Eval(nil)
}

func TestPreparedStringCalls(t *testing.T) {
	// Raw values retain invalid UTF-8; a JSON round trip may sanitize it.
	tests := []struct {
		name       string
		input      string
		delimiter  string
		wantBefore string
		wantTrim   string
	}{
		{
			name:       "empty input with empty delimiter",
			input:      "",
			delimiter:  "",
			wantBefore: "",
			wantTrim:   "",
		},
		{
			name:       "empty input with ASCII delimiter",
			input:      "",
			delimiter:  "i",
			wantBefore: "",
			wantTrim:   "",
		},
		{
			name:       "empty input with Unicode delimiter",
			input:      "",
			delimiter:  "界🎯",
			wantBefore: "",
			wantTrim:   "",
		},
		{
			name:       "empty input with NUL delimiter",
			input:      "",
			delimiter:  "\u0000",
			wantBefore: "",
			wantTrim:   "",
		},
		{
			name:       "empty input with invalid UTF-8 delimiter",
			input:      "",
			delimiter:  "\xff\xfe",
			wantBefore: "",
			wantTrim:   "",
		},
		{
			name:       "ASCII with empty delimiter",
			input:      "ascii",
			delimiter:  "",
			wantBefore: "",
			wantTrim:   "ascii",
		},
		{
			name:       "ASCII with ASCII delimiter",
			input:      "ascii",
			delimiter:  "i",
			wantBefore: "asc",
			wantTrim:   "asc",
		},
		{
			name:       "ASCII with Unicode delimiter",
			input:      "ascii",
			delimiter:  "界🎯",
			wantBefore: "ascii",
			wantTrim:   "ascii",
		},
		{
			name:       "ASCII with NUL delimiter",
			input:      "ascii",
			delimiter:  "\u0000",
			wantBefore: "ascii",
			wantTrim:   "ascii",
		},
		{
			name:       "ASCII with invalid UTF-8 delimiter",
			input:      "ascii",
			delimiter:  "\xff\xfe",
			wantBefore: "ascii",
			wantTrim:   "ascii",
		},
		{
			name:       "Unicode with empty delimiter",
			input:      "é界🎯",
			delimiter:  "",
			wantBefore: "",
			wantTrim:   "é界🎯",
		},
		{
			name:       "Unicode with ASCII delimiter",
			input:      "é界🎯",
			delimiter:  "i",
			wantBefore: "é界🎯",
			wantTrim:   "é界🎯",
		},
		{
			name:       "Unicode with Unicode delimiter",
			input:      "é界🎯",
			delimiter:  "界🎯",
			wantBefore: "é",
			wantTrim:   "é",
		},
		{
			name:       "Unicode with NUL delimiter",
			input:      "é界🎯",
			delimiter:  "\u0000",
			wantBefore: "é界🎯",
			wantTrim:   "é界🎯",
		},
		{
			name:       "Unicode with invalid UTF-8 delimiter",
			input:      "é界🎯",
			delimiter:  "\xff\xfe",
			wantBefore: "é界🎯",
			wantTrim:   "é界🎯",
		},
		{
			name:       "NUL with empty delimiter",
			input:      "a\u0000b",
			delimiter:  "",
			wantBefore: "",
			wantTrim:   "a\u0000b",
		},
		{
			name:       "NUL with ASCII delimiter",
			input:      "a\u0000b",
			delimiter:  "i",
			wantBefore: "a\u0000b",
			wantTrim:   "a\u0000b",
		},
		{
			name:       "NUL with Unicode delimiter",
			input:      "a\u0000b",
			delimiter:  "界🎯",
			wantBefore: "a\u0000b",
			wantTrim:   "a\u0000b",
		},
		{
			name:       "NUL with NUL delimiter",
			input:      "a\u0000b",
			delimiter:  "\u0000",
			wantBefore: "a",
			wantTrim:   "a\u0000b",
		},
		{
			name:       "NUL with invalid UTF-8 delimiter",
			input:      "a\u0000b",
			delimiter:  "\xff\xfe",
			wantBefore: "a\u0000b",
			wantTrim:   "a\u0000b",
		},
		{
			name:       "invalid UTF-8 with empty delimiter",
			input:      "bad\xff\xfe",
			delimiter:  "",
			wantBefore: "",
			wantTrim:   "bad\xff\xfe",
		},
		{
			name:       "invalid UTF-8 with ASCII delimiter",
			input:      "bad\xff\xfe",
			delimiter:  "i",
			wantBefore: "bad\xff\xfe",
			wantTrim:   "bad\xff\xfe",
		},
		{
			name:       "invalid UTF-8 with Unicode delimiter",
			input:      "bad\xff\xfe",
			delimiter:  "界🎯",
			wantBefore: "bad\xff\xfe",
			wantTrim:   "bad\xff\xfe",
		},
		{
			name:       "invalid UTF-8 with NUL delimiter",
			input:      "bad\xff\xfe",
			delimiter:  "\u0000",
			wantBefore: "bad\xff\xfe",
			wantTrim:   "bad\xff\xfe",
		},
		{
			name:       "invalid UTF-8 with invalid UTF-8 delimiter",
			input:      "bad\xff\xfe",
			delimiter:  "\xff\xfe",
			wantBefore: "bad",
			wantTrim:   "bad",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			functions := []struct {
				name string
				want string
			}{
				{name: "before", want: tt.wantBefore},
				{name: "trim_right", want: tt.wantTrim},
			}
			for _, function := range functions {
				t.Run(function.name, func(t *testing.T) {
					fn, ok := GetRegistry().Get(function.name)
					require.True(t, ok)
					values := []core.Value{str(tt.input), str(tt.delimiter)}
					args := []core.Argument{
						{Value: &core.ConstExpr{V: values[0]}},
						{Value: &core.ConstExpr{V: values[1]}},
					}
					resolved, err := fn.Resolve(values, nil)
					require.NoError(t, err)

					general, generalErr := fn.Call(resolved)
					prepared, preparedErr := preparedTestCall(t, fn, args)

					require.NoError(t, generalErr)
					require.NoError(t, preparedErr)
					require.Equal(t, str(function.want), general)
					require.Equal(t, general, prepared)
				})
			}
		})
	}
}

type indexedMockFn struct {
	mockFn
	indexed func([]core.Value) (core.Value, error)
}

func (f *indexedMockFn) CallIndexed(args []core.Value) (core.Value, error) { return f.indexed(args) }

func TestPreparedArgumentLayout(t *testing.T) {
	tests := []struct {
		name  string
		named []core.Argument
		want  []core.Value
	}{
		{
			name: "omitted defaults",
			want: []core.Value{str("input"), str("first default"), str("second default")},
		},
		{
			name: "out of parameter order and explicit null",
			named: []core.Argument{
				{Name: "second", Value: &core.ConstExpr{V: core.NullValue{}}},
				{Name: "first", Value: &core.ConstExpr{V: str("provided")}},
			},
			want: []core.Value{str("input"), str("provided"), core.NullValue{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, err := compile(&indexedMockFn{
				mockFn: mockFn{name: "layout", params: []Parameter{
					{Name: "value"},
					{Name: "first", Default: str("first default")},
					{Name: "second", Default: str("second default")},
				}},
				indexed: func(args []core.Value) (core.Value, error) {
					return core.ArrayValue{V: append([]core.Value(nil), args...)}, nil
				},
			})
			require.NoError(t, err)
			args := append([]core.Argument{{Value: &core.ConstExpr{V: str("input")}}}, tt.named...)

			got, err := preparedTestCall(t, fn, args)

			require.NoError(t, err)
			require.Equal(t, core.ArrayValue{V: tt.want}, got)
		})
	}
}

func TestPreparedCallWrapsFunctionError(t *testing.T) {
	t.Parallel()

	callErr := errors.New("operation failed")
	fn, err := compile(&indexedMockFn{
		mockFn: mockFn{name: "fail"},
		indexed: func([]core.Value) (core.Value, error) {
			return nil, callErr
		},
	})
	require.NoError(t, err)

	value, err := preparedTestCall(t, fn, nil)

	require.ErrorIs(t, err, callErr)
	require.EqualError(t, err, "test:1:2: fail: operation failed")
	require.Equal(t, core.NullValue{}, value)
}
