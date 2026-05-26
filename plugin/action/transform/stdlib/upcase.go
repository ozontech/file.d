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
			Required:      true,
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (upcase) Call(args map[string]core.Value) (core.Value, error) {
	val := args["value"].(core.StringValue)
	return core.StringValue{V: strings.ToUpper(val.V)}, nil
}
