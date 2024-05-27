package doif

import "fmt"

type comparisonOperation string

const (
	cmpOpLess           comparisonOperation = "lt"
	cmpOpLessOrEqual    comparisonOperation = "le"
	cmpOpGreater        comparisonOperation = "gt"
	cmpOpGreaterOrEqual comparisonOperation = "ge"
	cmpOpEqual          comparisonOperation = "eq"
	cmpOpNotEqual       comparisonOperation = "ne"
)

type comparator struct {
	cmpOp    comparisonOperation
	cmpValue int
}

func newComparator(cmpOp string, cmpValue int) (comparator, error) {
	typedCmpOp := comparisonOperation(cmpOp)
	switch typedCmpOp {
	case cmpOpLess, cmpOpLessOrEqual, cmpOpGreater, cmpOpGreaterOrEqual, cmpOpEqual, cmpOpNotEqual:
	default:
		return comparator{}, fmt.Errorf("unknown comparison operation: %s", typedCmpOp)
	}

	if cmpValue < 0 {
		return comparator{}, fmt.Errorf("negative cmp value: %d", cmpValue)
	}

	return comparator{
		cmpOp:    typedCmpOp,
		cmpValue: cmpValue,
	}, nil
}

func (c comparator) compare(value int) bool {
	switch c.cmpOp {
	case cmpOpLess:
		return value < c.cmpValue
	case cmpOpLessOrEqual:
		return value <= c.cmpValue
	case cmpOpGreater:
		return value > c.cmpValue
	case cmpOpGreaterOrEqual:
		return value >= c.cmpValue
	case cmpOpEqual:
		return value == c.cmpValue
	case cmpOpNotEqual:
		return value != c.cmpValue
	default:
		panic("invalid cmp op")
	}
}

func (c comparator) isEqualTo(other comparator) error {
	if c.cmpOp != other.cmpOp {
		return fmt.Errorf("unequal cmp operations: %s != %s", c.cmpOp, other.cmpOp)
	}

	if c.cmpValue != other.cmpValue {
		return fmt.Errorf("unequal cmp values: %d != %d", c.cmpValue, other.cmpValue)
	}

	return nil
}
