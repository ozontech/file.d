package doif

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type tsCmpMode int

const (
	// compare ts with current time
	cmpModeNow tsCmpMode = iota
	// compare ts with explicit value
	cmpModeExplicit
)

const (
	cmpModeNowTag      = "now"
	cmpModeExplicitTag = "explicit"
)

type tsCmpOpNode struct {
	fieldPath []string
	format    string

	cmpOp    comparisonOperation
	cmpMode  tsCmpMode
	cmpValue time.Time
}

func NewTsCmpOpNode(field string, format string, cmpOp string, cmpMode string, cmpValue time.Time) (Node, error) {
	resCmpOp := comparisonOperation(cmpOp)
	switch resCmpOp {
	case cmpOpLess, cmpOpLessOrEqual, cmpOpGreater, cmpOpGreaterOrEqual, cmpOpEqual, cmpOpNotEqual:
	default:
		return nil, fmt.Errorf("unknown comparison operation: %s", resCmpOp)
	}

	fieldPath := cfg.ParseFieldSelector(field)

	var resCmpMode tsCmpMode
	var resCmpValue time.Time

	switch cmpMode {
	case cmpModeNowTag:
		resCmpMode = cmpModeNow
	case cmpModeExplicitTag:
		resCmpMode = cmpModeExplicit
		resCmpValue = cmpValue
	default:
		return nil, fmt.Errorf("unknown ts cmp mode: %s", cmpMode)
	}

	return &tsCmpOpNode{
		fieldPath: fieldPath,
		format:    format,
		cmpOp:     resCmpOp,
		cmpMode:   resCmpMode,
		cmpValue:  resCmpValue,
	}, nil
}

func (n *tsCmpOpNode) Type() NodeType {
	return NodeTimestampCmpOp
}

func (n *tsCmpOpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if node == nil {
		return false
	}

	if !node.IsString() {
		return false
	}

	lhs, err := time.Parse(n.format, node.AsString())
	if err != nil {
		return false
	}

	var rhs time.Time
	switch n.cmpMode {
	case cmpModeNow:
		rhs = time.Now()
	case cmpModeExplicit:
		rhs = n.cmpValue
	default:
		panic(fmt.Sprintf("impossible: invalid cmp mode: %d", n.cmpMode))
	}

	switch n.cmpOp {
	case cmpOpLess:
		return lhs.Before(rhs)
	case cmpOpLessOrEqual:
		return lhs.Before(rhs) || lhs.Equal(rhs)
	case cmpOpGreater:
		return lhs.After(rhs)
	case cmpOpGreaterOrEqual:
		return lhs.After(rhs) || lhs.Equal(rhs)
	case cmpOpEqual:
		return lhs.Equal(rhs)
	case cmpOpNotEqual:
		return !lhs.Equal(rhs)
	default:
		panic(fmt.Sprintf("impossible: invalid cmp op: %s", n.cmpOp))
	}
}

func (n *tsCmpOpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*tsCmpOpNode)
	if !ok {
		return errors.New("nodes have different types; expected: tsCmpOpNode")
	}

	if n.format != n2Explicit.format {
		return fmt.Errorf("nodes have different time formats: %s != %s", n.format, n2Explicit.format)
	}

	if n.cmpOp != n2Explicit.cmpOp {
		return fmt.Errorf("nodes have different cmp ops: %s != %s", n.cmpOp, n2Explicit.cmpOp)
	}

	if n.cmpMode != n2Explicit.cmpMode {
		return fmt.Errorf("nodes have different cmp modes: %d != %d", n.cmpMode, n2Explicit.cmpMode)
	}

	if n.cmpValue != n2Explicit.cmpValue {
		return fmt.Errorf(
			"nodes have different cmp values: %s != %s",
			n.cmpValue.Format(time.RFC3339Nano),
			n2Explicit.cmpValue.Format(time.RFC3339Nano),
		)
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr; expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
