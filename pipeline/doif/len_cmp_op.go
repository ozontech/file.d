package doif

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ do-if-len-cmp-op-node
DoIf length comparison op node is considered to always be a leaf in the DoIf tree like DoIf field op node.
It contains operation that compares field length in bytes or array length (for array fields) with certain value.

Params:
  - `op` - must be `byte_len_cmp` or `array_len_cmp`. Required.
  - `field` - name of the field to apply operation. Required.
  - `cmp_op` - comparison operation name (see below). Required.
  - `value` - integer value to compare length with. Required non-negative.

Example 1 (byte length comparison):
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

Result:
```
{"pod_id":""}      # discarded
{"pod_id":123}     # discarded
{"pod_id":12345}   # not discarded
{"pod_id":123456}  # not discarded
```

Example 2 (array length comparison):

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

Result:
```
{"items":[]}         # discarded
{"items":[1]}        # discarded
{"items":[1, 2]}     # not discarded
{"items":[1, 2, 3]}  # not discarded
{"items":"1"}        # not discarded ('items' is not an array)
{"numbers":[1]}      # not discarded ('items' not found)
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
	lenCmpOp  lenCmpOpType
	fieldPath []string
	cmpOp     cmpOperation
	cmpValue  int
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

	if cmpValue < 0 {
		return nil, fmt.Errorf("negative cmp value: %d", cmpValue)
	}

	fieldPath := cfg.ParseFieldSelector(field)
	typedCmpOp, err := newCmpOp(cmpOp)
	if err != nil {
		return nil, fmt.Errorf("init byte len cmp op node: %w", err)
	}

	return &lenCmpOpNode{
		lenCmpOp:  lenCmpOp,
		fieldPath: fieldPath,
		cmpOp:     typedCmpOp,
		cmpValue:  cmpValue,
	}, nil
}

func (n *lenCmpOpNode) Type() NodeType {
	return NodeLengthCmpOp
}

func (n *lenCmpOpNode) Check(eventRoot *insaneJSON.Root) bool {
	value := 0

	switch n.lenCmpOp {
	case byteLenCmpOp:
		node := eventRoot.Dig(n.fieldPath...)
		if node == nil {
			return false
		}

		if node.IsObject() || node.IsArray() {
			value = len(node.EncodeToByte())
		} else {
			value = len(node.AsString())
		}
	case arrayLenCmpOp:
		node := eventRoot.Dig(n.fieldPath...)
		if !node.IsArray() {
			return false
		}

		value = len(node.AsArray())
	default:
		panic("impossible: bad len cmp op")
	}

	return n.cmpOp.compare(value, n.cmpValue)
}

func (n *lenCmpOpNode) isEqualTo(n2 Node, _ int) error {
	n2Explicit, ok := n2.(*lenCmpOpNode)
	if !ok {
		return errors.New("nodes have different types; expected: lenCmpOpNode")
	}

	if n.lenCmpOp != n2Explicit.lenCmpOp {
		return fmt.Errorf("nodes have different len cmp operations: %d != %d", n.lenCmpOp, n2Explicit.lenCmpOp)
	}

	if n.cmpOp != n2Explicit.cmpOp {
		return fmt.Errorf("nodes have different cmp operations")
	}

	if n.cmpValue != n2Explicit.cmpValue {
		return fmt.Errorf("nodes have different cmp values: %d != %d", n.cmpValue, n2Explicit.cmpValue)
	}

	if slices.Compare(n.fieldPath, n2Explicit.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr; expected: fieldPath=%v", n.fieldPath)
	}

	return nil
}
