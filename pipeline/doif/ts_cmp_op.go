package doif

import (
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type cmpValueChangingMode int

const (
	cmpValChModeConst cmpValueChangingMode = iota
	cmpValChModeNow
)

const (
	cmpValChModeConstTag = "const"
	cmpValChModeNowTag   = "now"
)

type tsCmpOpNode struct {
	fieldPath []string
	format    string

	cmpOp comparisonOperation

	cmpValChMode   cmpValueChangingMode
	constCmpValue  time.Time
	varCmpValue    atomic.Int64
	updateInterval time.Duration
}

func NewTsCmpOpNode(field string, format string, cmpOp string, cmpValChMode string, cmpValue time.Time, updateInterval time.Duration) (Node, error) {
	resCmpOp := comparisonOperation(cmpOp)
	switch resCmpOp {
	case cmpOpLess, cmpOpLessOrEqual, cmpOpGreater, cmpOpGreaterOrEqual, cmpOpEqual, cmpOpNotEqual:
	default:
		return nil, fmt.Errorf("unknown comparison operation: %s", resCmpOp)
	}

	fieldPath := cfg.ParseFieldSelector(field)

	var resCmpValChMode cmpValueChangingMode
	switch cmpValChMode {
	case cmpValChModeNowTag:
		resCmpValChMode = cmpValChModeNow
	case cmpValChModeConstTag:
		resCmpValChMode = cmpValChModeConst
	default:
		return nil, fmt.Errorf("unknown ts cmp mode: %s", cmpValChMode)
	}

	result := &tsCmpOpNode{
		fieldPath:      fieldPath,
		format:         format,
		cmpOp:          resCmpOp,
		cmpValChMode:   resCmpValChMode,
		constCmpValue:  cmpValue,
		updateInterval: updateInterval,
	}
	result.startUpdater(updateInterval)

	return result, nil
}

func (n *tsCmpOpNode) startUpdater(interval time.Duration) {
	if n.cmpValChMode == cmpValChModeNow {
		n.varCmpValue.Store(time.Now().UnixNano())
		go func() {
			for {
				n.varCmpValue.Store(time.Now().UnixNano())
				time.Sleep(interval)
			}
		}()
	}
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
	switch n.cmpValChMode {
	case cmpValChModeNow:
		rhs = time.Unix(0, n.varCmpValue.Load())
	case cmpValChModeConst:
		rhs = n.constCmpValue
	default:
		panic(fmt.Sprintf("impossible: invalid cmp value changing mode: %d", n.cmpValChMode))
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

	if n.cmpValChMode != n2Explicit.cmpValChMode {
		return fmt.Errorf("nodes have different cmp modes: %d != %d", n.cmpValChMode, n2Explicit.cmpValChMode)
	}

	if n.constCmpValue != n2Explicit.constCmpValue {
		return fmt.Errorf(
			"nodes have different cmp values: %s != %s",
			n.constCmpValue.Format(time.RFC3339Nano),
			n2Explicit.constCmpValue.Format(time.RFC3339Nano),
		)
	}

	if n.updateInterval != n2Explicit.updateInterval {
		return fmt.Errorf(
			"nodes have different update intervals: %v != %v",
			n.updateInterval, n2Explicit.updateInterval,
		)
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr; expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
