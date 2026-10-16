package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// the trim family strips characters from the ends of a string:
//
//	trim(.message, " ")         -> both ends
//	trim_left(.message, " ")    -> leading only
//	trim_right(.message, "\n")  -> trailing only
//
// cutset is a *set of characters*, not a substring: trim_right(v, "ms") removes
// every trailing "m" and "s", not the suffix "ms"; side is part of the function
// name rather than a mode argument so that a typo is caught when the program is
// compiled instead of on the first event that reaches it

type trim struct{}

func (trim) Name() string { return "trim" }

func (trim) Params() []Parameter { return trimParams("both ends") }

func (tr trim) Call(args map[string]core.Value) (core.Value, error) {
	return tr.call(args["value"], args["cutset"])
}

func (tr trim) CallIndexed(args []core.Value) (core.Value, error) {
	return tr.call(args[0], args[1])
}

func (trim) call(valueArg, cutsetArg core.Value) (core.Value, error) {
	value := valueArg.(core.StringValue).V
	cutset := cutsetArg.(core.StringValue).V
	return core.StringValue{V: strings.Trim(value, cutset)}, nil
}

type trimLeft struct{}

func (trimLeft) Name() string { return "trim_left" }

func (trimLeft) Params() []Parameter { return trimParams("the start") }

func (tr trimLeft) Call(args map[string]core.Value) (core.Value, error) {
	return tr.call(args["value"], args["cutset"])
}

func (tr trimLeft) CallIndexed(args []core.Value) (core.Value, error) {
	return tr.call(args[0], args[1])
}

func (trimLeft) call(valueArg, cutsetArg core.Value) (core.Value, error) {
	value := valueArg.(core.StringValue).V
	cutset := cutsetArg.(core.StringValue).V
	return core.StringValue{V: strings.TrimLeft(value, cutset)}, nil
}

type trimRight struct{}

func (trimRight) Name() string { return "trim_right" }

func (trimRight) Params() []Parameter { return trimParams("the end") }

func (tr trimRight) Call(args map[string]core.Value) (core.Value, error) {
	return tr.call(args["value"], args["cutset"])
}

func (tr trimRight) CallIndexed(args []core.Value) (core.Value, error) {
	return tr.call(args[0], args[1])
}

func (trimRight) call(valueArg, cutsetArg core.Value) (core.Value, error) {
	value := valueArg.(core.StringValue).V
	cutset := cutsetArg.(core.StringValue).V
	return core.StringValue{V: strings.TrimRight(value, cutset)}, nil
}

func trimParams(side string) []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to trim.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "cutset",
			Description:   "Set of characters to remove from " + side + "; any character in the set is stripped.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}
