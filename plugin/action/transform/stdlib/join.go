package stdlib

import (
	"fmt"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// join concatenates an array of strings into one string:
//
//	join(parse_regex_all(.log, r're\d+'), ",")   -> "re1,re2"
//
// only strings are joined; like the + operator, other kinds must be converted
// with to_string() first, so a surprising array does not silently produce a
// surprising field
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

func (j join) Call(args map[string]core.Value) (core.Value, error) {
	return j.call(nil, args["value"], args["separator"])
}

func (j join) CallIndexed(args []core.Value) (core.Value, error) {
	return j.call(nil, args[0], args[1])
}

func (j join) CallIndexedWithState(state *core.CallState, args []core.Value) (core.Value, error) {
	return j.call(state, args[0], args[1])
}

func (join) call(state *core.CallState, valueArg, separatorArg core.Value) (core.Value, error) {
	elements := valueArg.(core.ArrayValue).V
	separator := separatorArg.(core.StringValue).V

	// Neither case needs temporary storage or a new string.
	switch len(elements) {
	case 0:
		return core.StringValue{}, nil
	case 1:
		s, ok := elements[0].(core.StringValue)
		if !ok {
			return core.NullValue{}, fmt.Errorf(
				"element 0: expected string, got %s", elements[0].Kind())
		}
		return s, nil
	}

	parts := state.AcquireStrings(len(elements))
	defer state.ReleaseStrings(parts)
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
