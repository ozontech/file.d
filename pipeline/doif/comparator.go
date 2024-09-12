package doif

import "fmt"

type cmpOperation string

const (
	cmpOpLess           cmpOperation = "lt"
	cmpOpLessOrEqual    cmpOperation = "le"
	cmpOpGreater        cmpOperation = "gt"
	cmpOpGreaterOrEqual cmpOperation = "ge"
	cmpOpEqual          cmpOperation = "eq"
	cmpOpNotEqual       cmpOperation = "ne"
)

func newCmpOp(cmpOp string) (cmpOperation, error) {
	typedCmpOp := cmpOperation(cmpOp)
	switch typedCmpOp {
	case cmpOpLess, cmpOpLessOrEqual, cmpOpGreater, cmpOpGreaterOrEqual, cmpOpEqual, cmpOpNotEqual:
	default:
		return "", fmt.Errorf("unknown comparison operation: %s", typedCmpOp)
	}

	return typedCmpOp, nil
}

func (c cmpOperation) compare(lhs, rhs int) bool {
	switch c {
	case cmpOpLess:
		return lhs < rhs
	case cmpOpLessOrEqual:
		return lhs <= rhs
	case cmpOpGreater:
		return lhs > rhs
	case cmpOpGreaterOrEqual:
		return lhs >= rhs
	case cmpOpEqual:
		return lhs == rhs
	case cmpOpNotEqual:
		return lhs != rhs
	default:
		panic("invalid cmp op")
	}
}
