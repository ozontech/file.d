package doif

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ do-if-byte-len-cmp-op-node
DoIf byte length comparison op node is considered to always be a leaf in the DoIf tree like DoIf field op node.
It contains operation that compares field length in bytes with certain value.

Params:
  - `op` - must be `byte_len_cmp`. Required.
  - `field` - name of the field to apply operation. Required.
  - `cmp_op` - comparison operation name (see below). Required.
  - `value` - integer value to compare length with. Required non-negative.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: byte_len_cmp
          field: pod_id
          cmp_op: lt
          value: 5
```

result:
```
{"pod_id":""}      # discarded
{"pod_id":123}     # discarded
{"pod_id":12345}   # not discarded
{"pod_id":123456}  # not discarded
```

Possible values of field `cmp_op`: `lt`, `le`, `gt`, `ge`, `eq`, `ne`.
They denote corresponding comparison operations.

| Name | Op |
|------|----|
| `lt` | `<` |
| `le` | `<=` |
| `gt` | `>` |
| `ge` | `>=` |
| `eq` | `==` |
| `ne` | `!=` |
}*/

/*
type byteLengthCmpNode struct {
	fieldPath  []string
	comparator comparator
}

func NewByteLengthCmpNode(field string, cmpOp string, cmpValue int) (Node, error) {
	fieldPath := cfg.ParseFieldSelector(field)
	cmp, err := newComparator(cmpOp, cmpValue)
	if err != nil {
		return nil, fmt.Errorf("init byte len cmp op node: %w", err)
	}

	return &byteLengthCmpNode{
		fieldPath:  fieldPath,
		comparator: cmp,
	}, nil
}

func (n *byteLengthCmpNode) Type() NodeType {
	return NodeByteLenCmpOp
}

func (n *byteLengthCmpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if node == nil {
		return false
	}

	byteLen := 0
	if node.IsObject() || node.IsArray() {
		byteLen = len(node.EncodeToByte())
	} else {
		byteLen = len(node.AsString())
	}

	return n.comparator.compare(byteLen)
}

func (n *byteLengthCmpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*byteLengthCmpNode)
	if !ok {
		return errors.New("nodes have different types expected: byteLengthCmpNode")
	}

	if err := n.comparator.isEqualTo(n2Explicit.comparator); err != nil {
		return err
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
*/

/*{ do-if-array-len-cmp-op-node
DoIf array length comparison op node is also leaf in the DoIf tree like DoIf field op node and DoIf byte length cmp op node.
It contains operation that compares array length with certain value.

Params:
  - `op` - must be `array_len_cmp`. Required.
  - `field` - name of the field to apply operation. Required.
  - `cmp_op` - comparison operation name (see below). Required.
  - `value` - integer value to compare length with. Required non-negative.

Example:
```yaml
pipelines:
  test:
    actions:
      - type: discard
        do_if:
          op: array_len_cmp
          field: items
          cmp_op: lt
          value: 2
```

result:
```
{"items":[]}         # discarded
{"items":[1]}        # discarded
{"items":[1, 2]}     # not discarded
{"items":[1, 2, 3]}  # not discarded
{"items":"1"}        # not discarded ('items' is not an array)
{"numbers":[1]}      # not discarded ('items' not found)
```

Possible values of field `cmp_op` are the same as for byte length comparison op nodes
}*/

/*
type arrayLengthCmpNode struct {
	fieldPath  []string
	comparator comparator
}

func NewArrayLengthCmpNode(field string, cmpOp string, cmpValue int) (Node, error) {
	fieldPath := cfg.ParseFieldSelector(field)
	cmp, err := newComparator(cmpOp, cmpValue)
	if err != nil {
		return nil, fmt.Errorf("init array len cmp op node: %w", err)
	}

	return &arrayLengthCmpNode{
		fieldPath:  fieldPath,
		comparator: cmp,
	}, nil
}

func (n *arrayLengthCmpNode) Type() NodeType {
	return NodeArrayLenCmpOp
}

func (n *arrayLengthCmpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if !node.IsArray() {
		return false
	}

	return n.comparator.compare(len(node.AsArray()))
}

func (n *arrayLengthCmpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*arrayLengthCmpNode)
	if !ok {
		return errors.New("nodes have different types expected: arrayLengthCmpNode")
	}

	if err := n.comparator.isEqualTo(n2Explicit.comparator); err != nil {
		return err
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
*/

type lenCmpOpType int

const (
	byteLenCmpOp lenCmpOpType = iota
	arrayLenCmpOp
)

const (
	byteLenCmpOpTag  = "byte_len_cmp"
	arrayLenCmpOpTag = "array_len_cmp"
)

type lenCmpOpNode struct {
	lenCmpOp   lenCmpOpType
	fieldPath  []string
	comparator comparator
}

func NewLenCmpOpNode(op string, field string, cmpOp string, cmpValue int) (Node, error) {
	var lenCmpOp lenCmpOpType
	switch op {
	case byteLenCmpOpTag:
		lenCmpOp = byteLenCmpOp
	case arrayLenCmpOpTag:
		lenCmpOp = arrayLenCmpOp
	default:
		return nil, fmt.Errorf("bad len cmp op: %s", op)
	}

	fieldPath := cfg.ParseFieldSelector(field)
	cmp, err := newComparator(cmpOp, cmpValue)
	if err != nil {
		return nil, fmt.Errorf("init byte len cmp op node: %w", err)
	}

	return &lenCmpOpNode{
		lenCmpOp:   lenCmpOp,
		fieldPath:  fieldPath,
		comparator: cmp,
	}, nil
}

func (n *lenCmpOpNode) Type() NodeType {
	return NodeLenCmpOp
}

func (n *lenCmpOpNode) Check(eventRoot *insaneJSON.Root) bool {
	switch n.lenCmpOp {
	case byteLenCmpOp:
		node := eventRoot.Dig(n.fieldPath...)
		if node == nil {
			return false
		}

		byteLen := 0
		if node.IsObject() || node.IsArray() {
			byteLen = len(node.EncodeToByte())
		} else {
			byteLen = len(node.AsString())
		}

		return n.comparator.compare(byteLen)
	case arrayLenCmpOp:
		node := eventRoot.Dig(n.fieldPath...)
		if !node.IsArray() {
			return false
		}

		return n.comparator.compare(len(node.AsArray()))
	default:
		panic("impossible: bad len cmp op")
	}
}

func (n *lenCmpOpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*lenCmpOpNode)
	if !ok {
		return errors.New("nodes have different types; expected: lenCmpOpNode")
	}

	if n.lenCmpOp != n2Explicit.lenCmpOp {
		return fmt.Errorf("nodes have different len cmp operations: %d != %d", n.lenCmpOp, n2Explicit.lenCmpOp)
	}

	if err := n.comparator.isEqualTo(n2Explicit.comparator); err != nil {
		return err
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr; expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
