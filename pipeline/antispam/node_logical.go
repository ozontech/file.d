package antispam

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline/logic"
)

type logicalNode struct {
	op       logic.Op
	operands []Node
}

func newLogicalNode(op string, operands []Node) (*logicalNode, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}

	logicOp, err := logic.StringToOp(op)
	if err != nil {
		return nil, err
	}

	if logicOp == logic.Not {
		if len(operands) != 1 {
			return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
		}
	}

	return &logicalNode{
		op:       logicOp,
		operands: operands,
	}, nil
}

func (n *logicalNode) getType() nodeType {
	return nodeTypeLogical
}

func (n *logicalNode) check(event []byte, sourceName []byte, metadata map[string]string) bool {
	switch n.op {
	case logic.And:
		for _, op := range n.operands {
			if !op.check(event, sourceName, metadata) {
				return false
			}
		}
		return true
	case logic.Or:
		for _, op := range n.operands {
			if op.check(event, sourceName, metadata) {
				return true
			}
		}
		return false
	case logic.Not:
		return !n.operands[0].check(event, sourceName, metadata)
	default:
		panic("unknown logical op: %v")
	}
}
