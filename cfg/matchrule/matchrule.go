package matchrule

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strings"
)

type Mode byte

var (
	_ json.Unmarshaler = (*Mode)(nil)
)

func (m *Mode) UnmarshalJSON(i []byte) error {
	switch string(i) {
	case `"prefix"`:
		*m = ModePrefix
	case `"contains"`:
		*m = ModeContains
	case `"suffix"`:
		*m = ModeSuffix
	default:
		return fmt.Errorf("unknown mode %s", string(i))
	}
	return nil
}

const (
	ModePrefix Mode = iota
	ModeContains
	ModeSuffix
)

type Rule struct {
	// > @3@4@5@6
	// >
	// > Values to match at least one value.
	Values []string `json:"values" default:"" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > Matching mode. Allowed modes: `prefix`, `contains`, `suffix`. Defaults to `contains`.
	Mode Mode `json:"mode"` // *

	// > @3@4@5@6
	// >
	// > CaseInsensitive is the truth then Match results in a lowercase search value.
	// > Not available fo the ModeContains because performance issues may arise.
	CaseInsensitive bool `json:"case_insensitive"` // *

	// > @3@4@5@6
	// >
	// > Flag indicating whether to negate the result of matching rule.
	Invert bool `json:"invert"` // *

	minValueSize int
	maxValueSize int
}

func NewRule(values []string, mode Mode, caseInsensitive bool) Rule {
	if caseInsensitive && mode == ModeContains {
		panic(fmt.Errorf("сase insensitive for the 'contains' condition is not supported"))
	}

	for i := range values {
		if caseInsensitive {
			values[i] = strings.ToLower(values[i])
		}
	}

	return Rule{
		Values:          values,
		Mode:            mode,
		CaseInsensitive: caseInsensitive,
	}
}

func (e *Rule) calcValueSizes() {
	minValueSize := math.MaxInt
	maxValueSize := 0
	for i := range e.Values {
		if len(e.Values[i]) > maxValueSize {
			maxValueSize = len(e.Values[i])
		}
		if len(e.Values[i]) < minValueSize {
			minValueSize = len(e.Values[i])
		}
	}
	e.minValueSize = minValueSize
	e.maxValueSize = maxValueSize
}

func (e *Rule) Match(raw []byte) bool {
	ok := e.match(raw)
	if e.Invert {
		ok = !ok
	}
	return ok
}

func (e *Rule) match(raw []byte) bool {
	if e.maxValueSize == 0 {
		e.calcValueSizes()
	}
	if len(raw) < e.minValueSize {
		return false
	}

	var data []byte

	if e.Mode == ModeContains {
		data = raw
		if e.CaseInsensitive {
			data = bytes.ToLower(data)
		}
		for i := range e.Values {
			if len(data) < len(e.Values[i]) {
				continue
			}
			if bytes.Contains(data, []byte(e.Values[i])) {
				return true
			}
		}
		return false
	}

	var cutData []byte
	if len(raw) < e.maxValueSize {
		cutData = raw
	} else {
		switch e.Mode {
		case ModePrefix:
			cutData = raw[:e.maxValueSize]
		case ModeSuffix:
			cutData = raw[len(raw)-e.maxValueSize:]
		}
	}

	if e.CaseInsensitive {
		cutData = bytes.ToLower(cutData)
	}

	for _, value := range e.Values {
		if len(cutData) < len(value) {
			continue
		}
		switch e.Mode {
		case ModePrefix:
			data = cutData[:len(value)]
		case ModeSuffix:
			data = cutData[len(cutData)-len(value):]
		}

		if bytes.Equal(data, []byte(value)) {
			return true
		}
	}

	return false
}

type Cond byte

var _ json.Unmarshaler = (*Cond)(nil)

func (c *Cond) UnmarshalJSON(i []byte) error {
	switch string(i) {
	case `"and"`:
		*c = CondAnd
	case `"or"`:
		*c = CondOr
	default:
		return fmt.Errorf("unknown condition %s", string(i))
	}
	return nil
}

const (
	CondAnd Cond = iota
	CondOr
)

type RuleSet struct {
	// > @3@4@5@6
	// >
	// > Name of the ruleset.
	Name string `json:"name"` // *

	// > @3@4@5@6
	// >
	// > Condition for combining rules. Allowed values: `and`, `or`. Defaults to `and`.
	Cond Cond `json:"cond" default:"and" options:"and|or"` // *

	// > @3@4@5@6
	// >
	// > List of rules to check.
	Rules []Rule `json:"rules" default:"" slice:"true"` // *
}

func (e *RuleSet) Match(data []byte) bool {
	if len(e.Rules) == 0 {
		return false
	}

	for i := range e.Rules {
		rule := &e.Rules[i]

		match := rule.Match(data)
		if match && e.Cond == CondOr {
			return true
		}
		if !match && e.Cond == CondAnd {
			return false
		}
	}

	if e.Cond == CondAnd { // nolint: gosimple
		// all matches are true
		return true
	}

	return false
}
