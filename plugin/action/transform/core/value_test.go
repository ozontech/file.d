package core

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Debug output must not depend on Go map iteration order, so that the dump of a
// folded constant is reproducible. Value.String() is deliberately unsorted -
// it sits on the runtime path of string(value) - so the sort lives in dumpValue.
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

	root, err := insaneJSON.DecodeString(
		`{"num":1,"num2":1,"str":"1","obj":{"a":1},"arr":[1,2],"flt":1.0,"yes":true,"nul":null}`)
	require.NoError(t, err)
	defer insaneJSON.Release(root)

	node := func(field string) JSONNodeValue {
		return JSONNodeValue{N: root.Dig(field)}
	}

	tests := []struct {
		name  string
		left  Value
		right Value
		want  bool
	}{
		{
			name:  "object field equals an object literal",
			left:  node("obj"),
			right: ObjectValue{V: map[string]Value{"a": IntegerValue{V: 1}}},
			want:  true,
		},
		{
			name:  "object field differs from another object",
			left:  node("obj"),
			right: ObjectValue{V: map[string]Value{"a": IntegerValue{V: 2}}},
			want:  false,
		},
		{
			name:  "array field equals an array literal",
			left:  node("arr"),
			right: ArrayValue{V: []Value{IntegerValue{V: 1}, IntegerValue{V: 2}}},
			want:  true,
		},
		{
			name:  "array order matters",
			left:  node("arr"),
			right: ArrayValue{V: []Value{IntegerValue{V: 2}, IntegerValue{V: 1}}},
			want:  false,
		},
		{
			name:  "number equals an integer literal",
			left:  node("num"),
			right: IntegerValue{V: 1},
			want:  true,
		},
		{
			// Integers and floats compare numerically, as documented.
			name:  "integer equals a float",
			left:  node("num"),
			right: FloatValue{V: 1.0},
			want:  true,
		},
		{
			// Equality is type-aware; it used to be true via the string forms.
			name:  "number does not equal the same digits as a string",
			left:  node("num"),
			right: StringValue{V: "1"},
			want:  false,
		},
		{
			name:  "string equals a string literal",
			left:  node("str"),
			right: StringValue{V: "1"},
			want:  true,
		},
		{
			name:  "two nodes of equal value",
			left:  node("num"),
			right: node("num2"),
			want:  true,
		},
		{
			name:  "two nodes of different type",
			left:  node("num"),
			right: node("str"),
			want:  false,
		},
		{
			name:  "bool field",
			left:  node("yes"),
			right: BoolValue{V: true},
			want:  true,
		},
		{
			name:  "null field",
			left:  node("nul"),
			right: NullValue{},
			want:  true,
		},
		{
			name:  "missing field is null",
			left:  JSONNodeValue{N: nil},
			right: NullValue{},
			want:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.left.Equal(tt.right))
		})
	}
}
