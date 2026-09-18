package stdlib

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// find_all collects every match of a pattern in a string and returns them as an
// array. It is the multi-occurrence counterpart of capture: capture describes
// one match by its groups, find_all describes many matches by a single group.
//
//	find_all(.log, r'\d+')                     -> ["1", "42"]
//	join(find_all(.log, r're\d+', limit: 2), ",") -> "re1,re2"
//
// When nothing matches an empty array is returned, so the result is always safe
// to iterate or join.
type findAll struct{}

func (findAll) Name() string { return "find_all" }

func (findAll) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to search.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "pattern",
			Description:   "The regular expression to search for.",
			AcceptedKinds: []core.ValueKind{core.KindRegex},
		},
		{
			Name:          "group",
			Description:   "Index of the capture group to collect; 0 is the whole match.",
			Default:       core.IntegerValue{V: 0},
			AcceptedKinds: []core.ValueKind{core.KindInteger},
		},
		{
			Name:          "limit",
			Description:   "Maximum number of matches to collect; a negative limit collects all of them.",
			Default:       core.IntegerValue{V: -1},
			AcceptedKinds: []core.ValueKind{core.KindInteger},
		},
	}
}

func (findAll) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue).V
	re := args["pattern"].(core.RegexValue).V
	group := int(args["group"].(core.IntegerValue).V)
	limit := int(args["limit"].(core.IntegerValue).V)

	if group < 0 || group > re.NumSubexp() {
		return core.NullValue{}, fmt.Errorf(
			"group %d is out of range: pattern has %d capture groups", group, re.NumSubexp())
	}

	// A negative limit means "all matches"; Go spells that -1 exactly.
	if limit < 0 {
		limit = -1
	}

	// The index form is used rather than FindAllStringSubmatch so that a group
	// that did not participate in a match (-1) can be told apart from a group
	// that matched an empty string, and skipped.
	matches := re.FindAllStringSubmatchIndex(value, limit)

	result := make([]core.Value, 0, len(matches))
	for _, match := range matches {
		start, end := match[group*2], match[group*2+1]
		if start == -1 || end == -1 {
			continue
		}
		result = append(result, core.StringValue{V: value[start:end]})
	}

	return core.ArrayValue{V: result}, nil
}
