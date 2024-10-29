package doif

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ do-if-check-type-op-node
DoIf check type op node checks whether the type of the field node is the one from the list.

Params:
  - `op` - for that node the value is `check_type`
  - `field` - path to JSON node, can be empty string meaning root node, can be nested field `field.subfield` (if the field consists of `.` in the name, it must be shielded e.g. `exception\.type`)
  - `values` - list of types to check against. Allowed values are `obj`, `arr`, `number` (both ints or floats), `string`, `null`, `nil` (for the abscent fields).

Example:

```yaml
- type: discard
  do_if:
    op: not
    operands:
      - op: check_type
        field: log
        values: [obj, arr]
```

result:
```
{"log":{"message":"test"}}   # not discarded
{"log":[{"message":"test"}]} # not discarded
{"log":"test"}               # discarded
{"log":123}                  # discarded
{"log":null}                 # discarded
{"not_log":{"test":"test"}}  # discarded
```
}*/

var (
	checkTypeObjTag    = []byte("obj")
	checkTypeArrTag    = []byte("arr")
	checkTypeNumberTag = []byte("number")
	checkTypeStringTag = []byte("string")
	checkTypeNullTag   = []byte("null")
	checkTypeNilTag    = []byte("nil")
)

type checkTypeFn func(*insaneJSON.Node) bool

type checkTypeOpNode struct {
	fieldPath    []string
	fieldPathStr string
	checkTypeFns []checkTypeFn
}

func NewCheckTypeOpNode(field string, values [][]byte) (Node, error) {
	if len(values) == 0 {
		return nil, errors.New("values are not provided")
	}
	fieldPath := cfg.ParseFieldSelector(field)
	checkTypeFns := make([]checkTypeFn, 0)
	for _, val := range values {
		switch {
		case bytes.Equal(val, checkTypeObjTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsObject()
			})
		case bytes.Equal(val, checkTypeArrTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsArray()
			})
		case bytes.Equal(val, checkTypeNumberTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsNumber()
			})
		case bytes.Equal(val, checkTypeStringTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsString()
			})
		case bytes.Equal(val, checkTypeNullTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsNull()
			})
		case bytes.Equal(val, checkTypeNilTag):
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsNil()
			})
		default:
			return nil, fmt.Errorf(
				"invalid value for check_type: %q. Allowed values are: 'obj','arr','number','string','null','nil'",
				val,
			)
		}
	}
	return &checkTypeOpNode{
		fieldPath:    fieldPath,
		fieldPathStr: field,
		checkTypeFns: checkTypeFns,
	}, nil
}

func (n *checkTypeOpNode) Type() NodeType {
	return NodeCheckTypeOp
}

func (n *checkTypeOpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	for _, checkFn := range n.checkTypeFns {
		if checkFn(node) {
			return true
		}
	}
	return false
}

func (n *checkTypeOpNode) isEqualTo(n2 Node, _ int) error {
	n2f, ok := n2.(*checkTypeOpNode)
	if !ok {
		return errors.New("nodes have different types expected: checkTypeOpNode")
	}
	if n.fieldPathStr != n2f.fieldPathStr || slices.Compare[[]string](n.fieldPath, n2f.fieldPath) != 0 {
		return fmt.Errorf("nodes have different fieldPathStr expected: fieldPathStr=%q fieldPath=%v",
			n.fieldPathStr, n.fieldPath,
		)
	}
	if len(n.checkTypeFns) != len(n2f.checkTypeFns) {
		return fmt.Errorf("nodes have different checkTypes slices len expected: %d", len(n.checkTypeFns))
	}
	data := []byte(`{"obj":{"subfield":"test"},"arr":["test","test"],"number":123,"string":"test","null":null}`)
	root := insaneJSON.Spawn()
	if err := root.DecodeBytes(data); err != nil {
		panic(err)
	}
	nodes := map[string]*insaneJSON.Node{
		"obj":    root.Dig("obj"),
		"arr":    root.Dig("arr"),
		"number": root.Dig("number"),
		"string": root.Dig("string"),
		"null":   root.Dig("null"),
		"nil":    root.Dig("nil"),
	}
	for i := 0; i < len(n.checkTypeFns); i++ {
		for key, node := range nodes {
			res1 := n.checkTypeFns[i](node)
			res2 := n2f.checkTypeFns[i](node)
			if res1 != res2 {
				return fmt.Errorf("nodes have different check functions, different results for %q for funcs on position %d", key, i)
			}
		}
	}
	return nil
}
