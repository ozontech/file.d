package stdlib

import (
	"math"
	"regexp"
	"testing"
	"time"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

func TestToString(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		value core.Value
		want  string
	}
	tests := []testCase{
		{
			name:  "string passthrough",
			value: str("hello"),
			want:  "hello",
		},
		{
			name:  "raw Unicode NUL and invalid UTF-8",
			value: str("界\x00\xff"),
			want:  "界\x00\xff",
		},
		{
			name:  "integer",
			value: core.IntegerValue{V: 42},
			want:  "42",
		},
		{
			name:  "float",
			value: core.FloatValue{V: 1.5},
			want:  "1.5",
		},
		{
			name:  "negative zero",
			value: core.FloatValue{V: math.Copysign(0, -1)},
			want:  "-0",
		},
		{
			name:  "NaN",
			value: core.FloatValue{V: math.NaN()},
			want:  "NaN",
		},
		{
			name:  "positive infinity",
			value: core.FloatValue{V: math.Inf(1)},
			want:  "Infinity",
		},
		{
			name:  "negative infinity",
			value: core.FloatValue{V: math.Inf(-1)},
			want:  "-Infinity",
		},
		{
			name:  "true",
			value: core.BoolValue{V: true},
			want:  "true",
		},
		{
			name:  "false",
			value: core.BoolValue{V: false},
			want:  "false",
		},
		{
			name:  "null is empty at the top level",
			value: core.NullValue{},
		},
		{
			name:  "custom null kind is empty at the top level",
			value: keepInput{},
		},
		{
			name:  "empty array",
			value: core.ArrayValue{},
			want:  "[]",
		},
		{
			name:  "empty object",
			value: core.ObjectValue{},
			want:  "{}",
		},
		{
			name:  "array with reusable storage",
			value: core.ArrayValue{V: []core.Value{str("first"), core.IntegerValue{V: 42}, core.NullValue{}}},
			want:  `["first", 42, null]`,
		},
		{
			name: "nested collections keep null",
			value: core.ArrayValue{V: []core.Value{
				core.NullValue{},
				core.ObjectValue{V: map[string]core.Value{"values": core.ArrayValue{V: []core.Value{str("界"), core.IntegerValue{V: 42}, core.NullValue{}}}}},
				core.BoolValue{V: true},
			}},
			want: `[null, {"values": ["界", 42, null]}, true]`,
		},
		{
			name:  "array uses Go string quoting",
			value: core.ArrayValue{V: []core.Value{str("界\x00\xff\n\"\\")}},
			want:  `["界\x00\xff\n\"\\"]`,
		},
		{
			name:  "object uses Go key and value quoting",
			value: core.ObjectValue{V: map[string]core.Value{"key\x00\xff": str("value\n\xfe")}},
			want:  `{"key\x00\xff": "value\n\xfe"}`,
		},
		{
			name:  "regex",
			value: core.RegexValue{V: regexp.MustCompile(`a\d+`)},
			want:  `r'a\d+'`,
		},
		{
			name: "timestamp",
			value: core.TimestampValue{
				V: time.Date(2024, time.January, 2, 3, 4, 5, 123456789, time.FixedZone("UTC+3", 3*60*60)),
			},
			want: "2024-01-02T03:04:05.123456789+03:00",
		},
		{
			name:  "pointer to collection",
			value: &core.ArrayValue{V: []core.Value{str("value")}},
			want:  `["value"]`,
		},
		{
			name:  "pointer to string remains unquoted in a collection",
			value: core.ArrayValue{V: []core.Value{&core.StringValue{V: "value"}}},
			want:  "[value]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checkToStringCallModes(t, tt.value, tt.want, tt.want)
		})
	}
}

func TestToStringJSONNodeCallModes(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		input        string
		wantDirect   string
		wantPrepared string
	}
	tests := []testCase{
		{
			name:         "string",
			input:        `"界\nvalue"`,
			wantDirect:   "界\nvalue",
			wantPrepared: "界\nvalue",
		},
		{
			name:         "number materialization changes representation",
			input:        "1e2",
			wantDirect:   "1e2",
			wantPrepared: "100",
		},
		{
			name:  "null",
			input: "null",
		},
		{
			name:         "array materialized only by prepared call",
			input:        `["value", null]`,
			wantPrepared: `["value", null]`,
		},
		{
			name:         "object materialized only by prepared call",
			input:        `{"key": 1}`,
			wantPrepared: `{"key": 1}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, err := insaneJSON.DecodeString(tt.input)
			require.NoError(t, err)
			t.Cleanup(func() { insaneJSON.Release(node) })
			checkToStringCallModes(t, core.JSONNodeValue{N: node.Node}, tt.wantDirect, tt.wantPrepared)
		})
	}
	t.Run("nil node", func(t *testing.T) {
		t.Parallel()

		checkToStringCallModes(t, core.JSONNodeValue{}, "", "")
	})
}

func checkToStringCallModes(t *testing.T, value core.Value, wantDirect, wantPrepared string) {
	t.Helper()

	fn, err := compile(toString{})
	require.NoError(t, err)
	values := []core.Value{value}
	args := []core.Argument{
		{
			Value: &core.ConstExpr{V: value},
		},
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
		want string
	}
	modes := []callMode{
		{
			name: "general",
			call: func() (core.Value, error) {
				return callFn(toString{}, values, nil)
			},
			want: wantDirect,
		},
		{
			name: "indexed",
			call: func() (core.Value, error) {
				return toString{}.CallIndexed(values)
			},
			want: wantDirect,
		},
		{
			name: "stateful nil state",
			call: func() (core.Value, error) {
				return toString{}.CallIndexedWithState(nil, values)
			},
			want: wantDirect,
		},
		{
			name: "stateful reused state",
			call: func() (core.Value, error) {
				return toString{}.CallIndexedWithState(state, values)
			},
			want: wantDirect,
		},
		{
			name: "prepared nil state",
			call: func() (core.Value, error) {
				return expr.Eval(nil)
			},
			want: wantPrepared,
		},
		{
			name: "prepared reused state",
			call: func() (core.Value, error) {
				return expr.Eval(ctx)
			},
			want: wantPrepared,
		},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			for range 2 {
				got, callErr := mode.call()
				require.NoError(t, callErr)
				require.Equal(t, str(mode.want), got)
			}
		})
	}
}
