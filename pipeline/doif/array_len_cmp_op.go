package doif

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

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

type doIfArrayLengthCmpNode struct {
	fieldPath  []string
	comparator comparator
}

func NewArrayLengthCmpNode(field string, cmpOp string, cmpValue int) (DoIfNode, error) {
	fieldPath := cfg.ParseFieldSelector(field)
	cmp, err := newComparator(cmpOp, cmpValue)
	if err != nil {
		return nil, fmt.Errorf("init array len cmp op node: %w", err)
	}

	return &doIfArrayLengthCmpNode{
		fieldPath:  fieldPath,
		comparator: cmp,
	}, nil
}

func (n *doIfArrayLengthCmpNode) Type() DoIfNodeType {
	return DoIfNodeArrayLenCmpOp
}

func (n *doIfArrayLengthCmpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	if !node.IsArray() {
		return false
	}

	return n.comparator.compare(len(node.AsArray()))
}

func (n *doIfArrayLengthCmpNode) isEqualTo(n2 DoIfNode, _ int) error {
	n2Explicit, ok := n2.(*doIfArrayLengthCmpNode)
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
