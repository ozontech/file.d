package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

type Upcase struct{}

func (Upcase) Name() string { return "upcase" }

func (Upcase) Params() []core.Parameter {
	return []core.Parameter{
		{
			Name:          "value",
			Required:      true,
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (Upcase) Call(args map[string]core.Value) (core.Value, error) {
	val := args["value"].(core.StringValue)
	return core.StringValue{V: strings.ToUpper(val.V)}, nil
}
