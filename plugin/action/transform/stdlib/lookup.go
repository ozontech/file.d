package stdlib

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// lookup translates a value through a table:
//
//	api = {"0": "produce", "1": "fetch", "2": "offsets"}
//	.kafka_request_api_key = lookup(.kafka_request_api_key, api)
//
// The table is an ordinary object, so a constant one is built once at startup by
// the compiler's constant folding and a call costs a single map access.
//
// Keys are compared by their canonical string form, which makes the numeric 0
// and the string "0" the same key - JSON writes enumeration codes both ways.
//
// A value that is not in the table is returned unchanged, the same forgiving
// behavior as the substring family, so an unexpected code passes through
// instead of being dropped. Pass `default:` to override it.
type lookup struct{}

// keepInput is the default of the `default` parameter. A named parameter is
// made optional by having a non-nil default, and "return the input unchanged"
// is not expressible as an ordinary value - null already means an explicit
// null. This sentinel never leaves Call.
type keepInput struct{}

// paramDefault is the name of the miss-replacement parameter, used both in the
// signature and to read the resolved argument.
const paramDefault = "default"

func (keepInput) Kind() core.ValueKind { return core.KindNull }
func (keepInput) AsBool() bool         { return false }
func (keepInput) Equal(other core.Value) bool {
	_, ok := other.(keepInput)
	return ok
}
func (keepInput) String() string { return "<unchanged>" }

func (lookup) Name() string { return "lookup" }

func (lookup) Params() []Parameter {
	return []Parameter{
		{
			Name:        "value",
			Description: "The value to translate; it is matched against the table keys by its string form.",
			AcceptedKinds: []core.ValueKind{
				core.KindNull,
				core.KindBool,
				core.KindInteger,
				core.KindFloat,
				core.KindString,
			},
		},
		{
			Name:          "table",
			Description:   "An object mapping keys to their replacements.",
			AcceptedKinds: []core.ValueKind{core.KindObject},
		},
		{
			Name:        paramDefault,
			Description: "Returned when the value is not in the table; by default the value is returned unchanged.",
			Default:     keepInput{},
		},
	}
}

func (lookup) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"]
	table := args["table"].(core.ObjectValue)

	if mapped, ok := table.V[value.String()]; ok {
		return mapped, nil
	}

	if _, unchanged := args[paramDefault].(keepInput); unchanged {
		return value, nil
	}
	return args[paramDefault], nil
}
