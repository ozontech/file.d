package doif

import (
	"errors"
	"fmt"
	"slices"

	"github.com/ozontech/file.d/cfg"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ do-if-check-type-op-node
DoIf check type op node checks whether the type of the field node is the one from the list.

Params:
  - `op` - for that node the value is `check_type`
  - `field` - path to JSON node, can be empty string meaning root node, can be nested field `field.subfield` (if the field consists of `.` in the name, it must be shielded e.g. `exception\.type`)
  - `values` - list of types to check against. Allowed values are `object` (or `obj`), `array` (or `arr`), `number` (or `num`, matches both ints or floats), `string` (or `str`), `null`, `nil` (for the abscent fields). Values are deduplicated with respect to aliases on initialization to prevent redundant checks.

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

type checkTypeVal int

const (
	checkTypeUnknown checkTypeVal = iota
	checkTypeObj
	checkTypeArr
	checkTypeNumber
	checkTypeString
	checkTypeNull
	checkTypeNil
)

func (t checkTypeVal) String() string {
	switch t {
	case checkTypeObj:
		return "object"
	case checkTypeArr:
		return "array"
	case checkTypeNumber:
		return "number"
	case checkTypeString:
		return "string"
	case checkTypeNull:
		return "null"
	case checkTypeNil:
		return "nil"
	default:
		return "unknown" // nolint:goconst
	}
}

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
	usedTypesMap := make(map[checkTypeVal]struct{})
	checkTypeFns := make([]checkTypeFn, 0)
	for _, val := range values {
		switch string(val) {
		case "obj", "object":
			if _, ok := usedTypesMap[checkTypeObj]; ok {
				break
			}
			usedTypesMap[checkTypeObj] = struct{}{}
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsObject()
			})
		case "arr", "array":
			if _, ok := usedTypesMap[checkTypeArr]; ok {
				break
			}
			usedTypesMap[checkTypeArr] = struct{}{}
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsArray()
			})
		case "num", "number":
			if _, ok := usedTypesMap[checkTypeNumber]; ok {
				break
			}
			usedTypesMap[checkTypeNumber] = struct{}{}
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsNumber()
			})
		case "str", "string":
			if _, ok := usedTypesMap[checkTypeString]; ok {
				break
			}
			usedTypesMap[checkTypeString] = struct{}{}
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsString()
			})
		case "null":
			if _, ok := usedTypesMap[checkTypeNull]; ok {
				break
			}
			usedTypesMap[checkTypeNull] = struct{}{}
			checkTypeFns = append(checkTypeFns, func(n *insaneJSON.Node) bool {
				return n.IsNull()
			})
		case "nil":
			if _, ok := usedTypesMap[checkTypeNil]; ok {
				break
			}
			usedTypesMap[checkTypeNil] = struct{}{}
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
