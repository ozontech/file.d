package doif

import (
	"bytes"
	"errors"
	"fmt"
	insaneJSON "github.com/vitkovskii/insane-json"
)

// ! do-if-logical-op
// ^ do-if-logical-op

type doIfLogicalOpType int

const (
	doIfLogicalOpUnknown doIfLogicalOpType = iota
	doIfLogicalOr
	doIfLogicalAnd
	doIfLogicalNot
)

func (t doIfLogicalOpType) String() string {
	switch t {
	case doIfLogicalOr:
		return "or"
	case doIfLogicalAnd:
		return "and"
	case doIfLogicalNot:
		return "not"
	}
	return "unknown"
}

var (
	// > accepts at least one operand and returns true on the first returned true from its operands.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           op: or
	// >           operands:
	// >             - op: equal
	// >               field: pod
	// >               values: [test-pod-1, test-pod-2]
	// >             - op: equal
	// >               field: service
	// >               values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}     # discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # not discarded
	// > ```
	doIfLogicalOrBytes = []byte(`or`) // *

	// > accepts at least one operand and returns true if all operands return true
	// > (in other words returns false on the first returned false from its operands).
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           op: and
	// >           operands:
	// >             - op: equal
	// >               field: pod
	// >               values: [test-pod-1, test-pod-2]
	// >             - op: equal
	// >               field: service
	// >               values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # not discarded
	// > {"pod":"test-pod","service":"test-service"}     # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # not discarded
	// > ```
	doIfLogicalAndBytes = []byte(`and`) // *

	// > accepts exactly one operand and returns inverted result of its operand.
	// >
	// > Example:
	// > ```yaml
	// > pipelines:
	// >   test:
	// >     actions:
	// >       - type: discard
	// >         do_if:
	// >           op: not
	// >           operands:
	// >             - op: equal
	// >               field: service
	// >               values: [test-service]
	// > ```
	// >
	// > result:
	// > ```
	// > {"pod":"test-pod-1","service":"test-service"}   # not discarded
	// > {"pod":"test-pod-2","service":"test-service-2"} # discarded
	// > {"pod":"test-pod","service":"test-service"}     # not discarded
	// > {"pod":"test-pod","service":"test-service-1"}   # discarded
	// > ```
	doIfLogicalNotBytes = []byte(`not`) // *
)

/*{ do-if-logical-op-node
DoIf logical op node is a node considered to be the root or an edge between nodes.
It always has at least one operand which are other nodes and calls their checks
to apply logical operation on their results.

Params:
  - `op` - value from logical operations list. Required.
  - `operands` - list of another do-if nodes. Required non-empty.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: and
          operands:
            - op: equal
              field: pod
              values: [test-pod-1, test-pod-2]
              case_sensitive: true
            - op: equal
              field: service
              values: [test-service]
              case_sensitive: true
```

}*/

type doIfLogicalNode struct {
	op       doIfLogicalOpType
	operands []DoIfNode
}

func NewLogicalNode(op string, operands []DoIfNode) (DoIfNode, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}
	var lop doIfLogicalOpType
	opBytes := []byte(op)
	switch {
	case bytes.Equal(opBytes, doIfLogicalOrBytes):
		lop = doIfLogicalOr
	case bytes.Equal(opBytes, doIfLogicalAndBytes):
		lop = doIfLogicalAnd
	case bytes.Equal(opBytes, doIfLogicalNotBytes):
		lop = doIfLogicalNot
		if len(operands) > 1 {
			return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
		}
	default:
		return nil, fmt.Errorf("unknown logical op %q", op)
	}
	return &doIfLogicalNode{
		op:       lop,
		operands: operands,
	}, nil
}

func (n *doIfLogicalNode) Type() DoIfNodeType {
	return DoIfNodeLogicalOp
}

func (n *doIfLogicalNode) Check(eventRoot *insaneJSON.Root) bool {
	switch n.op {
	case doIfLogicalOr:
		for _, op := range n.operands {
			if op.Check(eventRoot) {
				return true
			}
		}
		return false
	case doIfLogicalAnd:
		for _, op := range n.operands {
			if !op.Check(eventRoot) {
				return false
			}
		}
		return true
	case doIfLogicalNot:
		return !n.operands[0].Check(eventRoot)
	}
	return false
}

func (n *doIfLogicalNode) isEqualTo(n2 DoIfNode, level int) error {
	n2l, ok := n2.(*doIfLogicalNode)
	if !ok {
		return errors.New("nodes have different types expected: logicalNode")
	}
	if n.op != n2l.op {
		return fmt.Errorf("nodes have different op expected: %q", n.op)
	}
	if len(n.operands) != len(n2l.operands) {
		return fmt.Errorf("nodes have different operands len expected: %d", len(n.operands))
	}
	for i := 0; i < len(n.operands); i++ {
		if err := n.operands[i].isEqualTo(n2l.operands[i], level+1); err != nil {
			tabs := make([]byte, 0, level)
			for j := 0; j < level; j++ {
				tabs = append(tabs, '\t')
			}
			return fmt.Errorf("nodes with op %q have different operand nodes on position %d:\n%s%w", n.op, i, tabs, err)
		}
	}
	return nil
}
