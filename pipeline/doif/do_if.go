package doif

import (
	insaneJSON "github.com/vitkovskii/insane-json"
)

// ! do-if-node
// ^ do-if-node

type DoIfNodeType int

const (
	DoIfNodeEmpty DoIfNodeType = iota

	// > Type of node where matching rules for fields are stored.
	DoIfNodeFieldOp // *

	// > Type of node where matching rules for byte lengths of fields are stored.
	DoIfNodeByteLenCmpOp // *

	// > Type of node where matching rules for array lengths are stored.
	DoIfNodeArrayLenCmpOp // *

	// > Type of node where logical rules for applying other rules are stored.
	DoIfNodeLogicalOp // *
)

type DoIfNode interface {
	Type() DoIfNodeType
	Check(*insaneJSON.Root) bool
	isEqualTo(DoIfNode, int) error
}

type DoIfChecker struct {
	root DoIfNode
}

func NewDoIfChecker(root DoIfNode) *DoIfChecker {
	return &DoIfChecker{
		root: root,
	}
}

func (c *DoIfChecker) IsEqualTo(c2 *DoIfChecker) error {
	return c.root.isEqualTo(c2.root, 1)
}

func (c *DoIfChecker) Check(eventRoot *insaneJSON.Root) bool {
	if eventRoot == nil {
		return false
	}
	return c.root.Check(eventRoot)
}
