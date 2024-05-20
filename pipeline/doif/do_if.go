package doif

import (
	insaneJSON "github.com/vitkovskii/insane-json"
)

// ! do-if-node
// ^ do-if-node

type NodeType int

const (
	NodeEmpty NodeType = iota

	// > Type of node where matching rules for fields are stored.
	NodeFieldOp // *

	// > Type of node where matching rules for byte lengths of fields are stored.
	NodeByteLenCmpOp // *

	// > Type of node where matching rules for array lengths are stored.
	NodeArrayLenCmpOp // *

	NodeLenCmpOp

	// > Type of node where logical rules for applying other rules are stored.
	NodeLogicalOp // *
)

type Node interface {
	Type() NodeType
	Check(*insaneJSON.Root) bool
	isEqualTo(Node, int) error
}

type Checker struct {
	root Node
}

func NewChecker(root Node) *Checker {
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
	return c.root.Check(eventRoot)
}
