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

type doIfByteLengthCmpNode struct {
	fieldPath  []string
	comparator comparator
}

func NewByteLengthCmpNode(field string, cmpOp string, cmpValue int) (DoIfNode, error) {
	fieldPath := cfg.ParseFieldSelector(field)
	cmp, err := newComparator(cmpOp, cmpValue)
	if err != nil {
		return nil, fmt.Errorf("init byte len cmp op node: %w", err)
	}

	return &doIfByteLengthCmpNode{
		fieldPath:  fieldPath,
		comparator: cmp,
	}, nil
}

func (n *doIfByteLengthCmpNode) Type() DoIfNodeType {
	return DoIfNodeByteLenCmpOp
}

func (n *doIfByteLengthCmpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if node == nil {
		return false
	}

	data := node.EncodeToByte()
	return n.comparator.compare(len(data))
}

func (n *doIfByteLengthCmpNode) isEqualTo(n2 DoIfNode, _ int) error {
	n2Explicit, ok := n2.(*doIfByteLengthCmpNode)
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
