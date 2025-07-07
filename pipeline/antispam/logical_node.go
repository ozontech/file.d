package antispam

import (
	"errors"
	"fmt"
)

type logicalOpType int

const (
	logicalAnd logicalOpType = iota
	logicalOr
	logicalNot
)

func (t logicalOpType) String() string {
	switch t {
	case logicalAnd:
		return "and"
	case logicalOr:
		return "or"
	case logicalNot:
		return "not"
	default:
		return "unknown"
	}
}

const (
	logicalAndTag = "and"
	logicalOrTag  = "or"
	logicalNotTag = "not"
)

type logicalNode struct {
	op       logicalOpType
	operands []Node
}

func newLogicalNode(op string, operands []Node) (Node, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}

	var lop logicalOpType
	switch op {
	case logicalOrTag:
		lop = logicalOr
	case logicalAndTag:
		lop = logicalAnd
	case logicalNotTag:
		lop = logicalNot
		if len(operands) > 1 {
			return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
		}
	default:
		return nil, fmt.Errorf("unknown logical op %q", op)
	}
	return &logicalNode{
		op:       lop,
		operands: operands,
	}, nil
}

func (n *logicalNode) Type() nodeType {
	return nodeTypeLogical
}

func (n *logicalNode) check(event []byte, sourceName []byte, metadata map[string]string) bool {
	switch n.op {
	case logicalAnd:
		for _, op := range n.operands {
			if !op.check(event, sourceName, metadata) {
				return false
			}
		}
		return true
	case logicalOr:
		for _, op := range n.operands {
			if op.check(event, sourceName, metadata) {
				return true
			}
		}
		return false

	case logicalNot:
		return !n.operands[0].check(event, sourceName, metadata)
	default:
		panic(fmt.Sprintf("unknown logical op: %v", n.op))
	}
}
