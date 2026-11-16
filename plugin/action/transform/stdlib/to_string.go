package stdlib

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// toString converts any value to its string representation, e.g. for building
// strings from non-string fields: "code " + to_string(.code)
//
// null converts to an empty string (not "null") so that templating over a
// missing field yields the same empty result as the modify plugin.
type toString struct{}

func (toString) Name() string { return "to_string" }

func (toString) Params() []Parameter {
	return []Parameter{
		{
			Name:        "value",
			Description: "The value to convert; any kind is accepted.",
		},
	}
}

func (s toString) Call(args map[string]core.Value) (core.Value, error) {
	return s.call(nil, args["value"])
}

func (s toString) CallIndexed(args []core.Value) (core.Value, error) {
	return s.call(nil, args[0])
}

func (s toString) CallIndexedWithState(state *core.CallState, args []core.Value) (core.Value, error) {
	return s.call(state, args[0])
}

func (toString) call(state *core.CallState, value core.Value) (core.Value, error) {
	switch value.Kind() {
	case core.KindNull:
		return core.StringValue{V: ""}, nil
	case core.KindArray, core.KindObject:
		return core.StringValue{V: core.FormatValue(value, state)}, nil
	default:
		return core.StringValue{V: value.String()}, nil
	}
}
