package transform

import (
	"strings"
)

type upcase struct{}

func (upcase) Name() string { return "upcase" }

func (upcase) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Required:      true,
			AcceptedKinds: []ValueKind{KindString},
		},
	}
}

func (upcase) Call(args map[string]Value) (Value, error) {
	val := args["value"].(StringValue)
	return StringValue{V: strings.ToUpper(val.V)}, nil
}
