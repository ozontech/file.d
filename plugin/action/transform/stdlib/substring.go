package stdlib

import (
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// The substring family extracts delimiter-bounded pieces of a string:
//
//	after(.log, " - ")        -> everything after the first " - "
//	before(.log, " [")        -> everything before the first " ["
//	between(.log, "[", "]")   -> text between the first "[" and the next "]"
//
// When a delimiter is not found, the value is returned unchanged - the same
// forgiving behavior as the modify plugin's trim_to filter, so a line in an
// unexpected format passes through instead of being emptied.

type after struct{}

func (after) Name() string { return "after" }

func (after) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to slice.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "separator",
			Description:   "Substring to search for; the result is everything after its first occurrence.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (after) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue).V
	sep := args["separator"].(core.StringValue).V

	idx := strings.Index(value, sep)
	if idx == -1 {
		return core.StringValue{V: value}, nil
	}
	return core.StringValue{V: value[idx+len(sep):]}, nil
}

type before struct{}

func (before) Name() string { return "before" }

func (before) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to slice.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "separator",
			Description:   "Substring to search for; the result is everything before its first occurrence.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (before) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue).V
	sep := args["separator"].(core.StringValue).V

	idx := strings.Index(value, sep)
	if idx == -1 {
		return core.StringValue{V: value}, nil
	}
	return core.StringValue{V: value[:idx]}, nil
}

type between struct{}

func (between) Name() string { return "between" }

func (between) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to slice.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "open",
			Description:   "Opening delimiter; the result starts after its first occurrence.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "close",
			Description:   "Closing delimiter; the result ends before its next occurrence after open.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func (between) Call(args map[string]core.Value) (core.Value, error) {
	value := args["value"].(core.StringValue).V
	open := args["open"].(core.StringValue).V
	closing := args["close"].(core.StringValue).V

	start := strings.Index(value, open)
	if start == -1 {
		return core.StringValue{V: value}, nil
	}
	rest := value[start+len(open):]

	end := strings.Index(rest, closing)
	if end == -1 {
		return core.StringValue{V: value}, nil
	}
	return core.StringValue{V: rest[:end]}, nil
}
