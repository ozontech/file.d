package stdlib

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// slice returns the part of a string between two positions, counted in
// characters rather than bytes so multi-byte text is never cut in half:
//
//	slice(.message, 0, end: 10)   -> the first 10 characters
//	slice(.message, -5)           -> the last 5 characters
//
// start is inclusive, end is exclusive, and both may be negative to count from
// the right. Positions outside the string are clamped instead of raising an
// error, so a shorter-than-expected line passes through whole.
type slice struct{}

func (slice) Name() string { return "slice" }

func (slice) Params() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			Description:   "The string to slice.",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "start",
			Description:   "Inclusive start position; negative counts from the end of the string.",
			AcceptedKinds: []core.ValueKind{core.KindInteger},
		},
		{
			Name:          "end",
			Description:   "Exclusive end position; negative counts from the end. Defaults to the end of the string.",
			Default:       core.NullValue{},
			AcceptedKinds: []core.ValueKind{core.KindInteger, core.KindNull},
		},
	}
}

func (slice) Call(args map[string]core.Value) (core.Value, error) {
	runes := []rune(args["value"].(core.StringValue).V)
	length := len(runes)

	start := clampIndex(int(args["start"].(core.IntegerValue).V), length)

	end := length
	if v, ok := args["end"].(core.IntegerValue); ok {
		end = clampIndex(int(v.V), length)
	}

	if end <= start {
		return core.StringValue{V: ""}, nil
	}
	return core.StringValue{V: string(runes[start:end])}, nil
}

// clampIndex resolves a possibly negative position against length and pins the
// result to [0, length].
func clampIndex(i, length int) int {
	if i < 0 {
		i += length
	}
	if i < 0 {
		return 0
	}
	if i > length {
		return length
	}
	return i
}
