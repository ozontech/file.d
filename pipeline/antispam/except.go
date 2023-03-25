package antispam

import (
	"bytes"
	"fmt"
	"strings"
)

//go:generate go run golang.org/x/tools/cmd/stringer@latest -type Condition -trimprefix Condition

type Condition byte

const (
	ConditionPrefix Condition = iota
	ConditionContains
	ConditionSuffix
)

type Exception struct {
	Value           string
	Condition       Condition
	CaseInsensitive bool
}

func NewException(value string, condition Condition, caseInsensitive bool) Exception {
	if caseInsensitive && condition == ConditionContains {
		panic(fmt.Errorf("сase insensitive for the 'contains' condtition is not supported"))
	}

	if caseInsensitive {
		value = strings.ToLower(value)
	}

	return Exception{
		Value:           value,
		Condition:       condition,
		CaseInsensitive: caseInsensitive,
	}
}

func (e *Exception) Match(raw []byte) bool {
	if e.Condition == ConditionContains {
		return bytes.Contains(raw, []byte(e.Value))
	}

	if len(raw) <= len(e.Value) {
		return false
	}

	var data []byte
	switch e.Condition {
	case ConditionPrefix:
		data = raw[:len(e.Value)]
	case ConditionSuffix:
		data = raw[len(raw)-len(e.Value):]
	}

	if e.CaseInsensitive {
		data = bytes.ToLower(data)
	}

	return bytes.Equal(data, []byte(e.Value))
}
