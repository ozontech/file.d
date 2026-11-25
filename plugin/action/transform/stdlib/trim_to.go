package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// the trim_to family cuts a string back to a delimiter, keeping the delimiter
// itself:
//
//	trim_to(.message, "{")        -> both ends
//	trim_to_left(.message, "{")   -> everything from the first "{"
//	trim_to_right(.message, "}")  -> everything up to the last "}"
//
// cutset is a *substring*, not a set of characters - that is what separates this
// family from trim; the left side searches the first occurrence and the right
// side the last one, so trim_to_right(trim_to_left(v, "{"), "}") spans the
// outermost pair rather than stopping inside a nested one, which is what the
// modify plugin's trim_to filter does and what extracting a JSON object out of a
// log line needs
//
// when the delimiter is not found the value is returned unchanged, the same
// forgiving behavior as the substring family, so a line in an unexpected format
// passes through instead of being emptied
//
// optional flag `include_cutset` allows to trim `cutset` itself

type trimTo struct{}

func (trimTo) Name() string { return "trim_to" }

func (trimTo) Params() []Parameter { return trimToParams("both ends") }

func (trimTo) Call(args map[string]core.Value) (core.Value, error) {
	value, cutset, includeCutset := trimToArgs(args)
	return core.StringValue{V: trimToRightOf(trimToLeftOf(value, cutset, includeCutset), cutset, includeCutset)}, nil
}

type trimToLeft struct{}

func (trimToLeft) Name() string { return "trim_to_left" }

func (trimToLeft) Params() []Parameter { return trimToParams("the start") }

func (trimToLeft) Call(args map[string]core.Value) (core.Value, error) {
	return core.StringValue{V: trimToLeftOf(trimToArgs(args))}, nil
}

type trimToRight struct{}

func (trimToRight) Name() string { return "trim_to_right" }

func (trimToRight) Params() []Parameter { return trimToParams("the end") }

func (trimToRight) Call(args map[string]core.Value) (core.Value, error) {
	return core.StringValue{V: trimToRightOf(trimToArgs(args))}, nil
}

// trimToLeftOf drops everything before the first occurrence of cutset
// the delimiter is part of the result (if `includeCutset` is not specified)
func trimToLeftOf(value, cutset string, includeCutset bool) string {
	if idx := strings.Index(value, cutset); idx != -1 {
		if includeCutset {
			idx += len(cutset)
		}
		return value[idx:]
	}
	return value
}

// trimToRightOf drops everything after the last occurrence of cutset
// the delimiter is part of the result (if `includeCutset` is not specified), hence idx+len(cutset) rather than idx+1:
// a multi-byte delimiter must not be cut in half
func trimToRightOf(value, cutset string, includeCutset bool) string {
	if idx := strings.LastIndex(value, cutset); idx != -1 {
		if includeCutset {
			return value[:idx]
		}
		return value[:idx+len(cutset)]
	}
	return value
}

func trimToParams(side string) []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to trim.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "cutset",
			Description:   "Substring to trim to from " + side + "; the delimiter itself is kept.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "include_cutset",
			Description:   "Allows to trim cutset itself. Defaults to false.",
			Default:       core.BoolValue{V: false},
			AcceptedKinds: []core.ValueKind{core.KindBool},
		},
	}
}

func trimToArgs(args map[string]core.Value) (string, string, bool) {
	includeCutset := false
	if v, ok := args["include_cutset"].(core.BoolValue); ok {
		includeCutset = v.V
	}
	return args["value"].(core.StringValue).V, args["cutset"].(core.StringValue).V, includeCutset
}
