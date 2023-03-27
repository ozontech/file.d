package antispam

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	// Values to match at least one value.
	Values []string `json:"values"`
	Mode   Mode     `json:"mode"`
	// CaseInsensitive is the truth then Match results in a lowercase search value.
	// Not available fo the ModeContains because performance issues may arise.
	CaseInsensitive bool `json:"case_insensitive"`
	Invert          bool `json:"invert"`
}

func NewRule(values []string, mode Mode, caseInsensitive bool) Rule {
	if caseInsensitive && mode == ModeContains {
		panic(fmt.Errorf("сase insensitive for the 'contains' condtition is not supported"))
	}

	if caseInsensitive {
		for i := range values {
			values[i] = strings.ToLower(values[i])
		}
	}

	return Rule{
		Values:          values,
		Mode:            mode,
		CaseInsensitive: caseInsensitive,
	}
}

func (e *Rule) Match(raw []byte) bool {
	ok := e.match(raw)
	if e.Invert {
		ok = !ok
	}
	return ok
}

func (e *Rule) match(raw []byte) bool {
	if e.Mode == ModeContains {
		for i := range e.Values {
			if bytes.Contains(raw, []byte(e.Values[i])) {
				return true
			}
		}
		return false
	}

	for _, value := range e.Values {
		if len(raw) < len(value) {
			continue
		}

		var data []byte
		switch e.Mode {
		case ModePrefix:
			data = raw[:len(value)]
		case ModeSuffix:
			data = raw[len(raw)-len(value):]
		}

		if e.CaseInsensitive {
			data = bytes.ToLower(data)
		}

		return bytes.Equal(data, []byte(value))
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

type Exception struct {
	Name  string `json:"name"`
	Cond  Cond   `json:"cond"`
	Rules []Rule `json:"rules"`
}

func (e *Exception) Match(data []byte) bool {
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

	if e.Cond == CondAnd {
		// all matches are true
		return true
	}

	return false
}
