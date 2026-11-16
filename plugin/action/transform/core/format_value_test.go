package core

import (
	"math"
	"regexp"
	"strings"
	"testing"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

type formatProbeValue struct {
	StringValue
	render func() string
}

func (v formatProbeValue) String() string {
	if v.render != nil {
		return v.render()
	}
	return "custom:" + v.V
}

func TestFormatValue(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		value Value
		want  string
	}
	tests := []testCase{
		{
			name:  "null",
			value: NullValue{},
			want:  "null",
		},
		{
			name:  "bool",
			value: BoolValue{V: true},
			want:  "true",
		},
		{
			name:  "integer",
			value: IntegerValue{V: -42},
			want:  "-42",
		},
		{
			name:  "float",
			value: FloatValue{V: 1.5},
			want:  "1.5",
		},
		{
			name:  "negative zero",
			value: FloatValue{V: math.Copysign(0, -1)},
			want:  "-0",
		},
		{
			name:  "NaN",
			value: FloatValue{V: math.NaN()},
			want:  "NaN",
		},
		{
			name:  "positive infinity",
			value: FloatValue{V: math.Inf(1)},
			want:  "Infinity",
		},
		{
			name:  "negative infinity",
			value: FloatValue{V: math.Inf(-1)},
			want:  "-Infinity",
		},
		{
			name:  "raw string",
			value: StringValue{V: "界\x00\xff\n"},
			want:  "界\x00\xff\n",
		},
		{
			name:  "regex",
			value: RegexValue{V: regexp.MustCompile(`a\d+`)},
			want:  `r'a\d+'`,
		},
		{
			name: "timestamp",
			value: TimestampValue{
				V: time.Date(2024, time.January, 2, 3, 4, 5, 123456789, time.FixedZone("UTC+3", 3*60*60)),
			},
			want: "2024-01-02T03:04:05.123456789+03:00",
		},
		{
			name:  "empty array",
			value: ArrayValue{},
			want:  "[]",
		},
		{
			name:  "empty object",
			value: ObjectValue{},
			want:  "{}",
		},
		{
			name: "nested collections and null",
			value: ArrayValue{V: []Value{
				NullValue{},
				ArrayValue{},
				ObjectValue{V: map[string]Value{"items": ArrayValue{V: []Value{IntegerValue{V: 7}, StringValue{V: "界"}}}}},
			}},
			want: `[null, [], {"items": [7, "界"]}]`,
		},
		{
			name:  "quoted array element",
			value: ArrayValue{V: []Value{StringValue{V: "界\x00\xff\n\"\\"}}},
			want:  `["界\x00\xff\n\"\\"]`,
		},
		{
			name:  "quoted object key and value",
			value: ObjectValue{V: map[string]Value{"key\x00\xff": StringValue{V: "value\n\xfe"}}},
			want:  `{"key\x00\xff": "value\n\xfe"}`,
		},
		{
			name:  "custom value",
			value: formatProbeValue{StringValue: StringValue{V: "raw"}},
			want:  "custom:raw",
		},
		{
			name:  "custom string kind remains unquoted in collections",
			value: ArrayValue{V: []Value{formatProbeValue{StringValue: StringValue{V: "raw"}}}},
			want:  "[custom:raw]",
		},
		{
			name:  "pointer to string remains unquoted in collections",
			value: ArrayValue{V: []Value{&StringValue{V: "raw"}}},
			want:  "[raw]",
		},
		{
			name:  "pointer to array",
			value: &ArrayValue{V: []Value{NullValue{}, StringValue{V: "value"}}},
			want:  `[null, "value"]`,
		},
		{
			name:  "pointer to object",
			value: &ObjectValue{V: map[string]Value{"key": IntegerValue{V: 7}}},
			want:  `{"key": 7}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			t.Cleanup(state.Reset)
			require.Equal(t, tt.want, tt.value.String())
			require.Equal(t, tt.want, FormatValue(tt.value, nil))
			for range 2 {
				require.Equal(t, tt.want, FormatValue(tt.value, &state))
			}
		})
	}
}

func TestFormatValueJSONNodeKeepsDirectStringSemantics(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		input string
		want  string
	}
	tests := []testCase{
		{
			name:  "string",
			input: `"界\nvalue"`,
			want:  "界\nvalue",
		},
		{
			name:  "raw number",
			input: "1e2",
			want:  "1e2",
		},
		{
			name:  "null",
			input: "null",
			want:  "null",
		},
		{
			name:  "array",
			input: `["value"]`,
		},
		{
			name:  "object",
			input: `{"key": 1}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			node, err := insaneJSON.DecodeString(tt.input)
			require.NoError(t, err)
			t.Cleanup(func() { insaneJSON.Release(node) })
			value := JSONNodeValue{N: node.Node}
			var state CallState
			require.Equal(t, tt.want, FormatValue(value, nil))
			require.Equal(t, tt.want, FormatValue(value, &state))
		})
	}
	require.Equal(t, "null", FormatValue(JSONNodeValue{}, &CallState{}))
}

func TestFormatValueObjectKeepsEveryField(t *testing.T) {
	t.Parallel()

	value := ObjectValue{V: map[string]Value{
		"first":  IntegerValue{V: 1},
		"second": IntegerValue{V: 2},
	}}
	want := []string{`{"first": 1, "second": 2}`, `{"second": 2, "first": 1}`}
	var state CallState
	for range 2 {
		// Map iteration order is deliberately unspecified, but no field may be lost.
		require.Contains(t, want, FormatValue(value, &state))
	}
}

func TestFormatValueReleasesStringBuffer(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		value      Value
		want       string
		wantFields []string
		wantPanic  bool
	}
	tests := []testCase{
		{
			name:  "array success",
			value: ArrayValue{V: []Value{StringValue{V: "first"}, StringValue{V: "second"}, StringValue{V: "third"}}},
			want:  `["first", "second", "third"]`,
		},
		{
			name: "object success",
			value: ObjectValue{V: map[string]Value{
				"first":  StringValue{V: "a"},
				"second": StringValue{V: "b"},
				"third":  StringValue{V: "c"},
			}},
			wantFields: []string{`"first": "a"`, `"second": "b"`, `"third": "c"`},
		},
		{
			name: "nested success",
			value: ArrayValue{V: []Value{
				StringValue{V: "first"},
				ObjectValue{V: map[string]Value{"nested": ArrayValue{V: []Value{StringValue{V: "second"}, StringValue{V: "third"}, StringValue{V: "fourth"}}}}},
				StringValue{V: "last"},
			}},
			want: `["first", {"nested": ["second", "third", "fourth"]}, "last"]`,
		},
		{
			name:      "array panic after partial fill",
			value:     ArrayValue{V: []Value{StringValue{V: "first"}, StringValue{V: "second"}, nil}},
			wantPanic: true,
		},
		{
			name: "object panic in nested array",
			value: ObjectValue{V: map[string]Value{
				"first":  StringValue{V: "a"},
				"second": StringValue{V: "b"},
				"nested": ArrayValue{V: []Value{StringValue{V: "first"}, StringValue{V: "second"}, nil}},
			}},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			t.Cleanup(state.Reset)
			buffer := state.AcquireStrings(8)
			state.ReleaseStrings(buffer)
			if tt.wantPanic {
				require.Panics(t, func() { FormatValue(tt.value, &state) })
			} else {
				got := FormatValue(tt.value, &state)
				if tt.wantFields != nil {
					require.True(t, strings.HasPrefix(got, "{") && strings.HasSuffix(got, "}"))
					require.ElementsMatch(t, tt.wantFields, strings.Split(got[1:len(got)-1], ", "))
				} else {
					require.Equal(t, tt.want, got)
				}
			}
			next := state.AcquireStrings(1)
			require.Same(t, &buffer[0], &next[0])
			require.Equal(t, make([]string, 8), next[:cap(next)])
			state.ReleaseStrings(next)
			require.Equal(t, `["after", true, false]`, FormatValue(ArrayValue{V: []Value{StringValue{V: "after"}, BoolValue{V: true}, BoolValue{V: false}}}, &state))
		})
	}
}

func TestFormatValueResultsSurviveNestedReuse(t *testing.T) {
	t.Parallel()

	var state CallState
	t.Cleanup(state.Reset)
	var nested string
	probe := formatProbeValue{
		render: func() string {
			nested = FormatValue(ArrayValue{V: []Value{StringValue{V: "first"}, StringValue{V: "second"}, StringValue{V: "third"}}}, &state)
			return nested
		},
	}
	first := FormatValue(ArrayValue{V: []Value{StringValue{V: "prefix"}, probe, StringValue{V: "suffix"}}}, &state)
	second := FormatValue(ArrayValue{V: []Value{StringValue{V: "replacement"}, IntegerValue{V: 42}, BoolValue{V: false}}}, &state)
	state.Reset()
	third := FormatValue(ArrayValue{V: []Value{NullValue{}, BoolValue{V: true}, StringValue{V: "last"}}}, &state)

	require.Equal(t, `["prefix", ["first", "second", "third"], "suffix"]`, first)
	require.Equal(t, `["first", "second", "third"]`, nested)
	require.Equal(t, `["replacement", 42, false]`, second)
	require.Equal(t, `[null, true, "last"]`, third)
}

func TestFormatValueBoundsRetainedStringStorage(t *testing.T) {
	t.Parallel()

	var state CallState
	small := state.AcquireStrings(4)
	state.ReleaseStrings(small)
	large := ArrayValue{V: make([]Value, maxRetainedStrings+1)}
	for i := range large.V {
		large.V[i] = BoolValue{V: true}
	}
	want := "[" + strings.Repeat("true, ", len(large.V)-1) + "true]"

	require.Equal(t, want, FormatValue(large, &state))
	require.Equal(t, 4, cap(state.stringBuffer))
	next := state.AcquireStrings(4)
	require.Same(t, &small[0], &next[0])
	require.Equal(t, make([]string, 4), next)
	state.ReleaseStrings(next)
}
