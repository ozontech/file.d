package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// replace returns a copy of the string with the first `count` non-overlapping instances of `old` replaced by `new`:
//
//	replace(.message, "old", "new", count: 3)   -> the first 3 instances
//	replace(.message, "old", "new")             -> all instances
//
// if `count < 0`, there is no limit on the number of replacements
//
// `new` can be empty, while `old` is not
type replace struct{}

func (replace) Name() string { return "replace" }

func (replace) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to replacing.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "old",
			Description:   "The substring that needs to be replaced",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "new",
			Description:   "Replacement substring; could be empty.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "count",
			Description:   "Maximum number of instances to replace; a negative for unlimited replacement.",
			Default:       core.IntegerValue{V: -1},
			AcceptedKinds: []core.ValueKind{core.KindInteger},
		},
	}
}

func (replace) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue).V
	old := args["old"].(core.StringValue).V
	new := args["new"].(core.StringValue).V
	count := int(args["count"].(core.IntegerValue).V)

	return core.StringValue{V: strings.Replace(value, old, new, count)}, nil
}
