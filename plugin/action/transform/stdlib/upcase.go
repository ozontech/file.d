package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

type upcase struct{}

func (upcase) Name() string { return "upcase" }

func (upcase) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to convert to upper case.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (u upcase) Call(args map[string]core.Value) (core.Value, error) {
	return u.call(args["value"])
}

func (u upcase) CallIndexed(args []core.Value) (core.Value, error) {
	return u.call(args[0])
}

func (upcase) call(valueArg core.Value) (core.Value, error) {
	val := valueArg.(core.StringValue)
	return core.StringValue{V: strings.ToUpper(val.V)}, nil
}
