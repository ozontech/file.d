package stdlib

import (
	"fmt"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// join concatenates an array of strings into one string:
//
//	join(find_all(.log, r're\d+'), ",")   -> "re1,re2"
//
// Only strings are joined; like the + operator, other kinds must be converted
// with string() first, so a surprising array does not silently produce a
// surprising field.
type join struct{}

func (join) Name() string { return "join" }

func (join) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The array of strings to join.",
			AcceptedKinds: []core.ValueKind{core.KindArray},
		},
		{
			Name:          "separator",
			Description:   `Placed between elements; pass "" to concatenate them directly.`,
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (join) Call(args map[string]core.Value) (core.Value, error) {
	elements := args["value"].(core.ArrayValue).V
	separator := args["separator"].(core.StringValue).V

	parts := make([]string, len(elements))
	for i, el := range elements {
		s, ok := el.(core.StringValue)
		if !ok {
			return core.NullValue{}, fmt.Errorf(
				"element %d: expected string, got %s", i, el.Kind())
		}
		parts[i] = s.V
	}

	return core.StringValue{V: strings.Join(parts, separator)}, nil
}
