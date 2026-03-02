package doif

// ! do-if-node
// ^ do-if-node

type NodeType int

const (
	NodeUnknownType NodeType = iota

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

type Data interface {
	Get(...string) []byte
}

type Node interface {
	Type() NodeType
	Check(Data) bool
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

func (c *Checker) Check(data Data) bool {
	return c.root.Check(data)
}
