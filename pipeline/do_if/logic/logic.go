package logic

import "fmt"

type Op int

const (
	And Op = iota
	Or
	Not
)

const (
	AndTag = "and"
	OrTag  = "or"
	NotTag = "not"
)

func (op Op) String() string {
	switch op {
	case And:
		return AndTag
	case Or:
		return OrTag
	case Not:
		return NotTag
	default:
		return "unknown"
	}
}

func StringToOp(s string) (Op, error) {
	switch s {
	case AndTag:
		return And, nil
	case OrTag:
		return Or, nil
	case NotTag:
		return Not, nil
	default:
		return -1, fmt.Errorf("unknown logic op tag: %s", s)
	}
}
