package stdlib

import (
	"strconv"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// capture matches value against a regular expression and returns an object of
// its named capture groups (keyed by group name). Unnamed groups are ignored.
// When value does not match, it returns null so callers can guard the result
// with `if m != null { ... }`.
//
// With numeric_groups enabled every group is additionally keyed by its index as
// a string - "0" is the whole match, "1" the first group and so on - which is
// how a pattern without named groups is read: m["1"].
type capture struct{}

func (capture) Name() string { return "capture" }

func (capture) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to match.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "pattern",
			Description:   "The regular expression; named groups (?P<name>...) become object keys.",
			AcceptedKinds: []core.ValueKind{core.KindRegex},
		},
		{
			Name:          "numeric_groups",
			Description:   `Also key every group by its index as a string: "0" is the whole match, "1" the first group.`,
			Default:       core.BoolValue{V: false},
			AcceptedKinds: []core.ValueKind{core.KindBool},
		},
	}
}

func (capture) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue)
	re := args["pattern"].(core.RegexValue).V
	numericGroups := args["numeric_groups"].(core.BoolValue).V

	match := re.FindStringSubmatch(value.V)
	if match == nil {
		return core.NullValue{}, nil
	}

	names := re.SubexpNames()
	groups := make(map[string]core.Value)
	for i, name := range names {
		if numericGroups {
			groups[strconv.Itoa(i)] = core.StringValue{V: match[i]}
		}
		// names[0] is the whole match (always unnamed); unnamed groups have "".
		if i == 0 || name == "" {
			continue
		}
		groups[name] = core.StringValue{V: match[i]}
	}

	return core.ObjectValue{V: groups}, nil
}
