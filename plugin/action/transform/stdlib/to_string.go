package stdlib

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// toString converts any value to its string representation, e.g. for building
// strings from non-string fields: "code " + string(.code).
//
// null converts to an empty string (not "null") so that templating over a
// missing field yields the same empty result as the modify plugin.
type toString struct{}

func (toString) Name() string { return "string" }

func (toString) Params() []Parameter {
	return []Parameter{
		{
			Name:        "value",
			Description: "The value to convert; any kind is accepted.",
		},
	}
}

func (toString) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"]
	if value.Kind() == core.KindNull {
		return core.StringValue{V: ""}, nil
	}
	return core.StringValue{V: value.String()}, nil
}
