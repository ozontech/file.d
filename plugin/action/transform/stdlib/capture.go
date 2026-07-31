package stdlib

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// capture matches value against a regular expression and returns an object of
// its named capture groups (keyed by group name). Unnamed groups are ignored.
// When value does not match, it returns null so callers can guard the result
// with `if m != null { ... }`.
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
	}
}

func (capture) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue)
	re := args["pattern"].(core.RegexValue).V

	match := re.FindStringSubmatch(value.V)
	if match == nil {
		return core.NullValue{}, nil
	}

	names := re.SubexpNames()
	groups := make(map[string]core.Value)
	for i, name := range names {
		// names[0] is the whole match (always unnamed); unnamed groups have "".
		if i == 0 || name == "" {
			continue
		}
		groups[name] = core.StringValue{V: match[i]}
	}

	return core.ObjectValue{V: groups}, nil
}
