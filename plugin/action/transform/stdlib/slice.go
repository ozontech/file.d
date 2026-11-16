package stdlib

import (
	"strings"
	"unicode/utf8"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// slice returns the part of a string between two positions, counted in
// characters rather than bytes so multi-byte text is never cut in half:
//
//	slice(.message, 0, end: 10)   -> the first 10 characters
//	slice(.message, -5)           -> the last 5 characters
//
// start is inclusive, end is exclusive, and both may be negative to count from
// the right; positions outside the string are clamped instead of raising an
// error, so a shorter-than-expected line passes through whole
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

func (s slice) Call(args map[string]core.Value) (core.Value, error) {
	return s.call(args["value"], args["start"], args["end"])
}

func (s slice) CallIndexed(args []core.Value) (core.Value, error) {
	return s.call(args[0], args[1], args[2])
}

func (slice) call(valueArg, startArg, endArg core.Value) (core.Value, error) {
	value := valueArg.(core.StringValue).V
	start := int(startArg.(core.IntegerValue).V)
	// The byte length is an upper bound on the number of runes. Using it for
	// the default end avoids counting the whole input for positive bounds.
	end := len(value)
	if v, ok := endArg.(core.IntegerValue); ok {
		end = int(v.V)
	}
	if start < 0 && end >= 0 && end < len(value) {
		// Mixed bounds can otherwise scan the entire input in both directions.
		length := utf8.RuneCountInString(value)
		start = max(0, start+length)
		if end >= length {
			end = len(value)
		}
	}
	if start >= 0 && end >= 0 && end <= start {
		return core.StringValue{V: ""}, nil
	}

	first := sliceByteOffset(value, start)
	var last int
	if start >= 0 && end >= start {
		// Continue from the start boundary rather than scanning the prefix twice.
		last = first + sliceByteOffset(value[first:], end-start)
	} else {
		last = sliceByteOffset(value, end)
	}
	if last <= first {
		return core.StringValue{V: ""}, nil
	}
	fragment := value[first:last]
	if utf8.ValidString(fragment) {
		// A short result must not retain the original string's backing storage.
		return core.StringValue{V: strings.Clone(fragment)}, nil
	}

	// Like []rune, range replaces each malformed byte with RuneError. Allocate
	// only the normalized result; valid RuneError characters stay intact.
	size := 0
	for _, r := range fragment {
		size += utf8.RuneLen(r)
	}
	var result strings.Builder
	result.Grow(size)
	for _, r := range fragment {
		if r == utf8.RuneError {
			result.WriteString("\ufffd")
		} else {
			result.WriteRune(r)
		}
	}
	return core.StringValue{V: result.String()}, nil
}

// sliceByteOffset locates a rune boundary, clamping at either end. Negative
// indices scan backwards so a short suffix does not need the total rune count.
func sliceByteOffset(value string, index int) int {
	if index < 0 {
		if index <= -len(value) {
			return 0
		}
		offset := len(value)
		for index < 0 && offset > 0 {
			_, size := utf8.DecodeLastRuneInString(value[:offset])
			offset -= size
			index++
		}
		return offset
	}
	if index >= len(value) {
		return len(value)
	}
	for offset := range value {
		if index == 0 {
			return offset
		}
		index--
	}
	return len(value)
}
