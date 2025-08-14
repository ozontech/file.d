package matchrule

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
	switch {
	case bytes.Equal(i, modePrefixBytes):
		*m = ModePrefix
	case bytes.Equal(i, modeContainsBytes):
		*m = ModeContains
	case bytes.Equal(i, modeSuffixBytes):
		*m = ModeSuffix
	default:
		return fmt.Errorf("unknown mode %s", string(i))
	}
	return nil
}

func (m *Mode) ToString() string {
	switch *m {
	case ModeContains:
		return "contains"
	case ModePrefix:
		return "prefix"
	case ModeSuffix:
		return "suffix"
	default:
		panic("unreachable")
	}
}

const (
	ModePrefix Mode = iota
	ModeContains
	ModeSuffix
)

var (
	modePrefixBytes   = []byte(`"prefix"`)
	modeContainsBytes = []byte(`"contains"`)
	modeSuffixBytes   = []byte(`"suffix"`)
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
	prepared     bool
}

func (r *Rule) GetMinValueSize() int {
	return r.minValueSize
}

func (r *Rule) GetMaxValueSize() int {
	return r.maxValueSize
}

func (r *Rule) Prepare() {
	if len(r.Values) == 0 {
		return
	}

	minValueSize := len(r.Values[0])
	maxValueSize := len(r.Values[0])
	for i := range r.Values {
		if r.CaseInsensitive {
			r.Values[i] = strings.ToLower(r.Values[i])
		}
		if len(r.Values[i]) > maxValueSize {
			maxValueSize = len(r.Values[i])
		}
		if len(r.Values[i]) < minValueSize {
			minValueSize = len(r.Values[i])
		}
	}

	r.minValueSize = minValueSize
	r.maxValueSize = maxValueSize
	r.prepared = true
}

func (r *Rule) Match(raw []byte) bool {
	if !r.prepared {
		panic("rule must be prepared")
	}

	ok := r.match(raw)
	if r.Invert {
		ok = !ok
	}
	return ok
}

func (r *Rule) match(raw []byte) bool {
	if len(raw) < r.minValueSize {
		return false
	}

	var data []byte

	if r.Mode == ModeContains {
		data = raw
		if r.CaseInsensitive {
			data = bytes.ToLower(data)
		}
		for i := range r.Values {
			if len(data) < len(r.Values[i]) {
				continue
			}
			if bytes.Contains(data, []byte(r.Values[i])) {
				return true
			}
		}
		return false
	}

	var cutData []byte
	if len(raw) < r.maxValueSize {
		cutData = raw
	} else {
		switch r.Mode {
		case ModePrefix:
			cutData = raw[:r.maxValueSize]
		case ModeSuffix:
			cutData = raw[len(raw)-r.maxValueSize:]
		}
	}

	if r.CaseInsensitive {
		cutData = bytes.ToLower(cutData)
	}

	for _, value := range r.Values {
		if len(cutData) < len(value) {
			continue
		}
		switch r.Mode {
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
	switch {
	case bytes.Equal(i, condAndBytes):
		*c = CondAnd
	case bytes.Equal(i, condOrBytes):
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

var (
	condAndBytes = []byte(`"and"`)
	condOrBytes  = []byte(`"or"`)
)

func (c *Cond) ToString() string {
	switch *c {
	case CondAnd:
		return "and"
	case CondOr:
		return "or"
	default:
		panic("unreachable")
	}
}

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

func (rs *RuleSet) Prepare() {
	for i := range rs.Rules {
		rs.Rules[i].Prepare()
	}
}

func (rs *RuleSet) Match(data []byte) bool {
	if len(rs.Rules) == 0 {
		return false
	}

	for i := range rs.Rules {
		rule := &rs.Rules[i]

		match := rule.Match(data)
		if match && rs.Cond == CondOr {
			return true
		}
		if !match && rs.Cond == CondAnd {
			return false
		}
	}

	if rs.Cond == CondAnd { // nolint: gosimple
		// all matches are true
		return true
	}

	return false
}

type RuleSets []RuleSet

func (rs RuleSets) Prepare() {
	for i := range rs {
		rs[i].Prepare()
	}
}
