package runtime

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"testing"
	"unicode/utf8"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

// valueToJSON supplies an owned string to tests of the JSON assignment path.
func valueToJSON(v core.Value) (string, error) {
	encoded, err := appendJSONValue(nil, v)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func checkAppendJSONValue(t *testing.T, value core.Value, want string) {
	t.Helper()

	const prefix = "prefix:\x00\xff"
	for _, reuse := range []bool{false, true} {
		capacity := len(prefix)
		if reuse {
			capacity += len(want)
		}
		dst := make([]byte, len(prefix), capacity)
		copy(dst, prefix)

		got, err := appendJSONValue(dst, value)

		require.NoError(t, err)
		require.Equal(t, prefix+want, string(got), "reuse capacity: %t", reuse)
		require.Equal(t, prefix, string(dst), "original prefix must stay unchanged")
	}
}

func TestAppendJSONValuePreservesPrefix(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		value core.Value
		want  string
	}
	tests := []testCase{
		{
			name:  "null",
			value: core.NullValue{},
			want:  "null",
		},
		{
			name:  "nil JSON node",
			value: core.JSONNodeValue{},
			want:  "null",
		},
		{
			name:  "nil native array stays an array",
			value: core.ArrayValue{},
			want:  "[]",
		},
		{
			name:  "nil native object stays an object",
			value: core.ObjectValue{},
			want:  "{}",
		},
		{
			name: "nested native values",
			value: core.ObjectValue{V: map[string]core.Value{
				"outer": core.ArrayValue{V: []core.Value{
					core.NullValue{},
					core.BoolValue{V: true},
					core.BoolValue{V: false},
					core.IntegerValue{V: math.MinInt64},
					core.IntegerValue{V: math.MaxInt64},
					core.ObjectValue{V: map[string]core.Value{
						"text": core.StringValue{V: "line\n界"},
					}},
				}},
			}},
			want: `{"outer":[null,true,false,-9223372036854775808,9223372036854775807,{"text":"line\n界"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			checkAppendJSONValue(t, tt.value, tt.want)
		})
	}
}

func TestAppendJSONValuePreservesFloatFormatting(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		value float64
		want  string
	}
	tests := []testCase{
		{
			name:  "positive zero",
			value: 0,
			want:  "0",
		},
		{
			name:  "negative zero",
			value: math.Copysign(0, -1),
			want:  "-0",
		},
		{
			name:  "fraction",
			value: 1.25,
			want:  "1.25",
		},
		{
			name:  "small number does not use an exponent",
			value: 1e-7,
			want:  "0.0000001",
		},
		{
			name:  "large number does not use an exponent",
			value: 1e20,
			want:  "100000000000000000000",
		},
		{
			name:  "smallest subnormal",
			value: math.SmallestNonzeroFloat64,
			want:  strconv.FormatFloat(math.SmallestNonzeroFloat64, 'f', -1, 64),
		},
		{
			name:  "largest finite number",
			value: math.MaxFloat64,
			want:  strconv.FormatFloat(math.MaxFloat64, 'f', -1, 64),
		},
		{
			name:  "NaN retains the legacy non-JSON spelling",
			value: math.NaN(),
			want:  "NaN",
		},
		{
			name:  "positive infinity",
			value: math.Inf(1),
			want:  "+Inf",
		},
		{
			name:  "negative infinity",
			value: math.Inf(-1),
			want:  "-Inf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			checkAppendJSONValue(t, core.FloatValue{V: tt.value}, tt.want)
		})
	}
}

func TestAppendJSONValueRejectsUnsupportedKinds(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		value core.Value
		want  string
	}
	tests := []testCase{
		{
			name:  "regex",
			value: core.RegexValue{},
			want:  "cannot serialize regex to JSON",
		},
		{
			name:  "timestamp",
			value: core.TimestampValue{},
			want:  "cannot serialize timestamp to JSON",
		},
		{
			name: "regex after an array element",
			value: core.ArrayValue{V: []core.Value{
				core.StringValue{V: "written first"},
				core.RegexValue{},
			}},
			want: "cannot serialize regex to JSON",
		},
		{
			name: "timestamp in an object",
			value: core.ObjectValue{V: map[string]core.Value{
				"value": core.TimestampValue{},
			}},
			want: "cannot serialize timestamp to JSON",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			const prefix = "existing prefix"
			dst := make([]byte, len(prefix), 128)
			copy(dst, prefix)

			_, err := appendJSONValue(dst, tt.value)

			require.EqualError(t, err, tt.want)
			require.Equal(t, prefix, string(dst))
		})
	}
}

func TestAppendJSONValuePreservesNodeEncoding(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name        string
		input       string
		wantRaw     string
		mutateValue string
	}
	tests := []testCase{
		{
			name:    "raw decimal",
			input:   `{"value":1.50}`,
			wantRaw: "1.50",
		},
		{
			name:    "raw exponent",
			input:   `{"value":1E+03}`,
			wantRaw: "1E+03",
		},
		{
			name:    "raw negative zero",
			input:   `{"value":-0}`,
			wantRaw: "-0",
		},
		{
			name:  "node escaping and nested raw numbers",
			input: `{"value":{"<>&":"\u2028\u2029\n\t\"\\/界","nested":[1.500,1e3,-0,9007199254740993]}}`,
		},
		{
			name:        "mutated node with invalid UTF-8",
			input:       `{"value":""}`,
			mutateValue: "\xff<>&\x00\u2028\u2029界",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root, release := newJSONRoot(t, tt.input)
			defer release()
			node := root.Dig("value")
			require.NotNil(t, node)
			if tt.mutateValue != "" {
				node.MutateToString(tt.mutateValue)
			}
			want := node.EncodeToString()
			if tt.wantRaw != "" {
				require.Equal(t, tt.wantRaw, want)
			}

			checkAppendJSONValue(t, core.JSONNodeValue{N: node}, want)
			checkAppendJSONValue(t, core.ArrayValue{V: []core.Value{core.JSONNodeValue{N: node}}}, "["+want+"]")
		})
	}
}

func checkAppendJSONStringMatchesMarshal(t *testing.T, input string) {
	t.Helper()

	want, err := json.Marshal(input)
	require.NoError(t, err)
	const prefix = "prefix:\x00\xff"
	for _, reuse := range []bool{false, true} {
		capacity := len(prefix)
		if reuse {
			capacity += len(want)
		}
		dst := make([]byte, len(prefix), capacity)
		copy(dst, prefix)

		got := appendJSONString(dst, input)

		require.Equal(t, prefix+string(want), string(got), "input: %q; reuse capacity: %t", input, reuse)
		require.Equal(t, prefix, string(dst))
	}
	checkAppendJSONValue(t, core.StringValue{V: input}, string(want))

	wantObject, err := json.Marshal(map[string]string{input: input})
	require.NoError(t, err)
	checkAppendJSONValue(t, core.ObjectValue{V: map[string]core.Value{
		input: core.StringValue{V: input},
	}}, string(wantObject))
}

func TestAppendJSONStringMatchesMarshal(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		input string
	}
	tests := []testCase{
		{
			name:  "empty",
			input: "",
		},
		{
			name:  "quote slash and backslash",
			input: "\"/\\",
		},
		{
			name:  "HTML escaping",
			input: "<script>&text</script>",
		},
		{
			name:  "Unicode and line separators",
			input: "é界🙂\u2028\u2029",
		},
		{
			name:  "valid replacement rune",
			input: "a\ufffdb",
		},
		{
			name:  "invalid and overlong sequences",
			input: "\xff\xfe\x80\xc0\xaf\xe0\x80\xaf\xf0\x80\x80\xaf",
		},
		{
			name:  "surrogate and out of range sequences",
			input: "\xed\xa0\x80\xed\xbf\xbf\xf4\x90\x80\x80\xf5\x80\x80\x80",
		},
		{
			name:  "truncated sequences beside escapes",
			input: "\xc2\"\xe2\x82\\\xf0\x9f\x99<\xe2\x82",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			checkAppendJSONStringMatchesMarshal(t, tt.input)
		})
	}
}

func TestAppendJSONStringByteAndRuneBoundaries(t *testing.T) {
	t.Parallel()

	allBytes := make([]byte, 256)
	for i := range allBytes {
		allBytes[i] = byte(i)
		checkAppendJSONStringMatchesMarshal(t, "界\""+string(allBytes[i:i+1])+"\\<tail")
	}
	checkAppendJSONStringMatchesMarshal(t, string(allBytes))

	boundaries := []rune{0x7f, 0x80, 0x7ff, 0x800, 0xd7ff, 0xe000, 0xffff, 0x10000, utf8.MaxRune, '\u2028', '\u2029', utf8.RuneError}
	for _, boundary := range boundaries {
		t.Run(fmt.Sprintf("U+%04X", boundary), func(t *testing.T) {
			encoded := string(boundary)
			for end := 0; end <= len(encoded); end++ {
				checkAppendJSONStringMatchesMarshal(t, "prefix<"+encoded[:end]+"\"suffix")
				checkAppendJSONStringMatchesMarshal(t, encoded[:end])
			}
		})
	}
}

func FuzzAppendJSONStringMatchesMarshal(f *testing.F) {
	seeds := []string{
		"",
		"plain ASCII",
		"\x00\b\f\n\r\t\x1f\"/\\<>&",
		"é界🙂\u2028\u2029",
		"\xff\xc0\xaf\xed\xa0\x80\xf4\x90\x80\x80\xe2\x82",
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		checkAppendJSONStringMatchesMarshal(t, input)
	})
}
