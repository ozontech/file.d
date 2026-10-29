package core

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Debug output must not depend on Go map iteration order, so that the dump of a
// folded constant is reproducible. Value.String() is deliberately unsorted -
// it sits on the runtime path of to_string(value) - so the sort lives in dumpValue
func TestDumpConstIsStable(t *testing.T) {
	t.Parallel()

	expr := &ConstExpr{V: ObjectValue{V: map[string]Value{
		"z": StringValue{V: "z"},
		"a": IntegerValue{V: 1},
		"m": ArrayValue{V: []Value{BoolValue{V: true}}},
		"n": ObjectValue{V: map[string]Value{"b": IntegerValue{V: 2}, "a": IntegerValue{V: 1}}},
	}}}

	want := `Const({"a": 1, "m": [true], "n": {"a": 1, "b": 2}, "z": "z"})`
	for range 20 {
		assert.Equal(t, want, DumpAST(expr, 0))
	}
}

// A JSON node is compared by its decoded value, not by its text. Comparing
// String() forms made a field never equal an object or array literal, since a
// node renders as raw JSON while an ObjectValue renders with spaces.
func TestJSONNodeEquality(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		leftField  string
		rightField string
		right      Value
		want       bool
	}
	tests := []testCase{
		{
			name:      "object field equals an object literal",
			leftField: "obj",
			right:     ObjectValue{V: map[string]Value{"a": IntegerValue{V: 1}}},
			want:      true,
		},
		{
			name:      "object field differs from another object",
			leftField: "obj",
			right:     ObjectValue{V: map[string]Value{"a": IntegerValue{V: 2}}},
			want:      false,
		},
		{
			name:      "array field equals an array literal",
			leftField: "arr",
			right:     ArrayValue{V: []Value{IntegerValue{V: 1}, IntegerValue{V: 2}}},
			want:      true,
		},
		{
			name:      "array order matters",
			leftField: "arr",
			right:     ArrayValue{V: []Value{IntegerValue{V: 2}, IntegerValue{V: 1}}},
			want:      false,
		},
		{
			name:      "number equals an integer literal",
			leftField: "num",
			right:     IntegerValue{V: 1},
			want:      true,
		},
		{
			// Integers and floats compare numerically, as documented.
			name:      "integer equals a float",
			leftField: "num",
			right:     FloatValue{V: 1.0},
			want:      true,
		},
		{
			// Equality is type-aware; it used to be true via the string forms.
			name:      "number does not equal the same digits as a string",
			leftField: "num",
			right:     StringValue{V: "1"},
			want:      false,
		},
		{
			name:      "string equals a string literal",
			leftField: "str",
			right:     StringValue{V: "1"},
			want:      true,
		},
		{
			name:       "two nodes of equal value",
			leftField:  "num",
			rightField: "num2",
			want:       true,
		},
		{
			name:       "two nodes of different type",
			leftField:  "num",
			rightField: "str",
			want:       false,
		},
		{
			name:      "bool field",
			leftField: "yes",
			right:     BoolValue{V: true},
			want:      true,
		},
		{
			name:      "null field",
			leftField: "nul",
			right:     NullValue{},
			want:      true,
		},
		{
			name:      "missing field is null",
			leftField: "missing",
			right:     NullValue{},
			want:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Each parallel case owns its tree because reading nodes can mutate them.
			root, err := insaneJSON.DecodeString(
				`{"num":1,"num2":1,"str":"1","obj":{"a":1},"arr":[1,2],"flt":1.0,"yes":true,"nul":null}`)
			require.NoError(t, err)
			defer insaneJSON.Release(root)

			left := JSONNodeValue{N: root.Dig(tt.leftField)}
			right := tt.right
			if tt.rightField != "" {
				right = JSONNodeValue{N: root.Dig(tt.rightField)}
			}
			assert.Equal(t, tt.want, left.Equal(right))
		})
	}
}
