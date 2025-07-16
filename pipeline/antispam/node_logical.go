package antispam

import (
	"github.com/ozontech/file.d/pipeline/logic"
)

type logicalNode struct {
	op       logic.Op
	operands []Node
}

func newLogicalNode(op logic.Op, operands []Node) Node {
	return &logicalNode{
		op:       op,
		operands: operands,
	}
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
