package doif

import (
	insaneJSON "github.com/ozontech/insane-json"
)

// ! do-if-node
// ^ do-if-node

type nodeType int

const (
	NodeUnknownType nodeType = iota

	// > Type of node where matching rules for fields are stored.
	NodeFieldOp // *

	// > Type of node where matching rules for byte length and array length are stored.
	NodeLengthCmpOp // *

	// > Type of node where matching rules for timestamps are stored.
	NodeTimestampCmpOp // *

	// > Type of node where matching rules for check types are stored.
	NodeCheckTypeOp // *

	// > Type of node where logical rules for applying other rules are stored.
	NodeLogicalOp // *
)

type Node interface {
	Type() nodeType
	check(*insaneJSON.Root) bool
	isEqualTo(Node, int) error
}

type Checker struct {
	root Node
}

func newChecker(root Node) *Checker {
	return &Checker{
		root: root,
	}
}

func (c *Checker) IsEqualTo(c2 *Checker) error {
	return c.root.isEqualTo(c2.root, 1)
}

func (c *Checker) Check(eventRoot *insaneJSON.Root) bool {
	if eventRoot == nil {
		return false
	}
	return c.root.check(eventRoot)
}
