package do_if

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/pipeline/logic"
	insaneJSON "github.com/ozontech/insane-json"
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

/*{ do-if-logical-op
Operation `or` accepts at least one operand and returns true on the first returned true from its operands.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: or
          operands:
            - op: equal
              field: pod
              values: [test-pod-1, test-pod-2]
            - op: equal
              field: service
              values: [test-service]
```

Result:
```
{"pod":"test-pod-1","service":"test-service"}   # discarded
{"pod":"test-pod-2","service":"test-service-2"} # discarded
{"pod":"test-pod","service":"test-service"}     # discarded
{"pod":"test-pod","service":"test-service-1"}   # not discarded
```

<br>

Operation `and` accepts at least one operand and returns true if all operands return true
(in other words returns false on the first returned false from its operands).

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
            - op: equal
              field: service
              values: [test-service]
```

Result:
```
{"pod":"test-pod-1","service":"test-service"}   # discarded
{"pod":"test-pod-2","service":"test-service-2"} # not discarded
{"pod":"test-pod","service":"test-service"}     # not discarded
{"pod":"test-pod","service":"test-service-1"}   # not discarded
```

<br>

Operation `not` accepts exactly one operand and returns inverted result of its operand.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: not
          operands:
            - op: equal
              field: service
              values: [test-service]
```

Result:
```
{"pod":"test-pod-1","service":"test-service"}   # not discarded
{"pod":"test-pod-2","service":"test-service-2"} # discarded
{"pod":"test-pod","service":"test-service"}     # not discarded
{"pod":"test-pod","service":"test-service-1"}   # discarded
```

<br>

}*/

type logicalNode struct {
	op       logic.Op
	operands []Node
}

func newLogicalNode(op string, operands []Node) (Node, error) {
	if len(operands) == 0 {
		return nil, errors.New("logical op must have at least one operand")
	}

	logicOp, err := logic.StringToOp(op)
	if err != nil {
		return nil, err
	}

	if logicOp == logic.Not && len(operands) != 1 {
		return nil, fmt.Errorf("logical not must have exactly one operand, got %d", len(operands))
	}

	return &logicalNode{
		op:       logicOp,
		operands: operands,
	}, nil
}

func (n *logicalNode) Type() nodeType {
	return NodeLogicalOp
}

func (n *logicalNode) checkEvent(eventRoot *insaneJSON.Root) bool {
	switch n.op {
	case logic.Or:
		for _, op := range n.operands {
			if op.checkEvent(eventRoot) {
				return true
			}
		}
		return false
	case logic.And:
		for _, op := range n.operands {
			if !op.checkEvent(eventRoot) {
				return false
			}
		}
		return true
	case logic.Not:
		return !n.operands[0].checkEvent(eventRoot)
	default:
		panic("unknown logical op")
	}
}

func (n *logicalNode) checkRaw(event []byte, sourceName []byte, metadata map[string]string) bool {
	switch n.op {
	case logic.Or:
		for _, op := range n.operands {
			if op.checkRaw(event, sourceName, metadata) {
				return true
			}
		}
		return false
	case logic.And:
		for _, op := range n.operands {
			if !op.checkRaw(event, sourceName, metadata) {
				return false
			}
		}
		return true
	case logic.Not:
		return !n.operands[0].checkRaw(event, sourceName, metadata)
	default:
		panic("unknown logical op")
	}
}

func (n *logicalNode) isEqualTo(n2 Node, level int) error {
	n2l, ok := n2.(*logicalNode)
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
