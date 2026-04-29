package transform

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	insaneJSON "github.com/ozontech/insane-json"
)

type ValueKind int

const (
	KindNull ValueKind = iota
	KindBool
	KindInteger
	KindFloat
	KindString
	KindArray
	KindObject
	KindRegex
	KindTimestamp
)

var valueStrings []string = []string{"null", "bool", "integer", "float", "string", "array", "object", "regex", "timestamp"}

func (k ValueKind) String() string {
	if int(k) < len(valueStrings) {
		return valueStrings[k]
	}
	return "unknown"
}

type Value interface {
	Kind() ValueKind
	AsBool() bool
	Equal(Value) bool
	String() string
}

type NullValue struct{}
type BoolValue struct{ V bool }
type IntegerValue struct{ V int64 }
type FloatValue struct{ V float64 }
type StringValue struct{ V string }
type ArrayValue struct{ V []Value }
type ObjectValue struct{ V map[string]Value }
type RegexValue struct{ V *regexp.Regexp }
type TimestampValue struct{ V time.Time }
type JSONNodeValue struct{ N *insaneJSON.Node }

func (NullValue) Kind() ValueKind      { return KindNull }
func (BoolValue) Kind() ValueKind      { return KindBool }
func (IntegerValue) Kind() ValueKind   { return KindInteger }
func (FloatValue) Kind() ValueKind     { return KindFloat }
func (StringValue) Kind() ValueKind    { return KindString }
func (ArrayValue) Kind() ValueKind     { return KindArray }
func (ObjectValue) Kind() ValueKind    { return KindObject }
func (RegexValue) Kind() ValueKind     { return KindRegex }
func (TimestampValue) Kind() ValueKind { return KindTimestamp }
func (v JSONNodeValue) Kind() ValueKind {
	switch {
	case v.N == nil || v.N.IsNull():
		return KindNull
	case v.N.IsTrue() || v.N.IsFalse():
		return KindBool
	case v.N.IsNumber():
		if _, err := strconv.ParseInt(v.N.AsString(), 10, 64); err == nil {
			return KindInteger
		}
		return KindFloat
	case v.N.IsString():
		return KindString
	case v.N.IsArray():
		return KindArray
	case v.N.IsObject():
		return KindObject
	}
	return KindNull
}

func (NullValue) AsBool() bool       { return false }
func (v BoolValue) AsBool() bool     { return v.V }
func (IntegerValue) AsBool() bool    { return true }
func (FloatValue) AsBool() bool      { return true }
func (StringValue) AsBool() bool     { return true }
func (ArrayValue) AsBool() bool      { return true }
func (ObjectValue) AsBool() bool     { return true }
func (RegexValue) AsBool() bool      { return true }
func (TimestampValue) AsBool() bool  { return true }
func (v JSONNodeValue) AsBool() bool { return v.N.AsBool() }

func (NullValue) Equal(other Value) bool { return other.Kind() == KindNull }

func (v BoolValue) Equal(other Value) bool {
	o, ok := other.(BoolValue)
	return ok && v.V == o.V
}

func (v IntegerValue) Equal(other Value) bool {
	switch o := other.(type) {
	case IntegerValue:
		return v.V == o.V
	case FloatValue:
		return float64(v.V) == o.V
	}
	return false
}

func (v FloatValue) Equal(other Value) bool {
	switch o := other.(type) {
	case FloatValue:
		return v.V == o.V
	case IntegerValue:
		return v.V == float64(o.V)
	}
	return false
}

func (v StringValue) Equal(other Value) bool {
	o, ok := other.(StringValue)
	return ok && v.V == o.V
}

func (v ArrayValue) Equal(other Value) bool {
	o, ok := other.(ArrayValue)
	if !ok || len(v.V) != len(o.V) {
		return false
	}
	for i := range v.V {
		if !v.V[i].Equal(o.V[i]) {
			return false
		}
	}
	return true
}

func (v ObjectValue) Equal(other Value) bool {
	o, ok := other.(ObjectValue)
	if !ok || len(v.V) != len(o.V) {
		return false
	}
	for k, lhs := range v.V {
		rhs, exists := o.V[k]
		if !exists || !lhs.Equal(rhs) {
			return false
		}
	}
	return true
}

func (v RegexValue) Equal(other Value) bool {
	o, ok := other.(RegexValue)
	return ok && v.V.String() == o.V.String()
}

func (v TimestampValue) Equal(other Value) bool {
	o, ok := other.(TimestampValue)
	return ok && v.V.Equal(o.V)
}

func (v JSONNodeValue) Equal(other Value) bool {
	return v.String() == other.String()
}

func (NullValue) String() string { return "null" }

func (v BoolValue) String() string {
	if v.V {
		return "true"
	}
	return "false"
}

func (v IntegerValue) String() string {
	return strconv.FormatInt(v.V, 10)
}

func (v FloatValue) String() string {
	f := v.V
	switch {
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case math.IsNaN(f):
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func (v StringValue) String() string { return v.V }

func (v ArrayValue) String() string {
	parts := make([]string, len(v.V))
	for i, el := range v.V {
		parts[i] = display(el)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (v ObjectValue) String() string {
	parts := make([]string, 0, len(v.V))
	for k, val := range v.V {
		parts = append(parts, fmt.Sprintf("%q: %s", k, display(val)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func (v RegexValue) String() string {
	return "r'" + v.V.String() + "'"
}

func (v TimestampValue) String() string {
	return v.V.Format(time.RFC3339Nano)
}

func (v JSONNodeValue) String() string {
	if v.N == nil {
		return "null"
	}
	return v.N.AsString()
}

func ToFloat(v Value) (float64, error) {
	switch t := v.(type) {
	case IntegerValue:
		return float64(t.V), nil
	case FloatValue:
		return t.V, nil
	}
	return 0, fmt.Errorf("type error: expected integer or float, got %s", v.Kind())
}

// wraps StringValue in quotes when used inside array/object formatting.
func display(v Value) string {
	if s, ok := v.(StringValue); ok {
		return fmt.Sprintf("%q", s.V)
	}
	return v.String()
}

func resolve(v Value) Value {
	jv, ok := v.(JSONNodeValue)
	if !ok {
		return v
	}
	return jsonNodeToValue(jv.N)
}

// jsonNodeToValue converts a single insaneJSON node to a Value.
// The conversion is recursive for arrays and objects.
func jsonNodeToValue(node *insaneJSON.Node) Value {
	if node == nil {
		return NullValue{}
	}

	switch {
	case node.IsNull():
		return NullValue{}
	case node.IsTrue() || node.IsFalse():
		return BoolValue{V: node.AsBool()}
	case node.IsNumber():
		if i, err := strconv.ParseInt(node.AsString(), 10, 64); err == nil {
			return IntegerValue{V: i}
		}
		f, _ := strconv.ParseFloat(node.AsString(), 64)
		return FloatValue{V: f}
	case node.IsString():
		return StringValue{V: node.AsString()}
	case node.IsArray():
		nodes := node.AsArray()
		arr := make([]Value, len(nodes))
		for i, n := range nodes {
			arr[i] = jsonNodeToValue(n)
		}
		return ArrayValue{V: arr}
	case node.IsObject():
		fields := node.AsFields()
		obj := make(map[string]Value, len(fields))
		for _, field := range fields {
			key := field.AsString()
			val := node.Dig(key)
			obj[key] = jsonNodeToValue(val)
		}
		return ObjectValue{V: obj}
	}
	return NullValue{}
}
