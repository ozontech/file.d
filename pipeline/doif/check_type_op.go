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

type checkType int

const (
	checkTypeUnknown checkType = iota
	checkTypeObj
	checkTypeArr
	checkTypeNumber
	checkTypeString
	checkTypeNull
	checkTypeNil
)

var (
	checkTypeUnknownTag = []byte("unknown")
	checkTypeObjTag     = []byte("obj")
	checkTypeArrTag     = []byte("arr")
	checkTypeNumberTag  = []byte("number")
	checkTypeStringTag  = []byte("string")
	checkTypeNullTag    = []byte("null")
	checkTypeNilTag     = []byte("nil")
)

func (t checkType) String() string {
	switch t {
	case checkTypeObj:
		return string(checkTypeObjTag)
	case checkTypeArr:
		return string(checkTypeArrTag)
	case checkTypeNumber:
		return string(checkTypeNumberTag)
	case checkTypeString:
		return string(checkTypeStringTag)
	case checkTypeNull:
		return string(checkTypeNullTag)
	case checkTypeNil:
		return string(checkTypeNilTag)
	default:
		return string(checkTypeUnknownTag)
	}
}

type checkTypeOpNode struct {
	fieldPath    []string
	fieldPathStr string
	checkTypes   []checkType
}

func NewCheckTypeOpNode(field string, values [][]byte) (Node, error) {
	if len(values) == 0 {
		return nil, errors.New("values are not provided")
	}
	fieldPath := cfg.ParseFieldSelector(field)
	checkTypes := make([]checkType, 0)
	for _, val := range values {
		switch {
		case bytes.Equal(val, checkTypeObjTag):
			checkTypes = append(checkTypes, checkTypeObj)
		case bytes.Equal(val, checkTypeArrTag):
			checkTypes = append(checkTypes, checkTypeArr)
		case bytes.Equal(val, checkTypeNumberTag):
			checkTypes = append(checkTypes, checkTypeNumber)
		case bytes.Equal(val, checkTypeStringTag):
			checkTypes = append(checkTypes, checkTypeString)
		case bytes.Equal(val, checkTypeNullTag):
			checkTypes = append(checkTypes, checkTypeNull)
		case bytes.Equal(val, checkTypeNilTag):
			checkTypes = append(checkTypes, checkTypeNil)
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
		checkTypes:   checkTypes,
	}, nil
}

func (n *checkTypeOpNode) Type() NodeType {
	return NodeCheckTypeOp
}

func (n *checkTypeOpNode) Check(eventRoot *insaneJSON.Root) bool {
	node := eventRoot.Dig(n.fieldPath...)
	for _, t := range n.checkTypes {
		switch {
		case t == checkTypeNull && node.IsNull():
			return true
		case t == checkTypeObj && node.IsObject():
			return true
		case t == checkTypeArr && node.IsArray():
			return true
		case t == checkTypeNumber && node.IsNumber():
			return true
		case t == checkTypeString && node.IsString():
			return true
		case t == checkTypeNil && node.IsNil():
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
	if len(n.checkTypes) != len(n2f.checkTypes) {
		return fmt.Errorf("nodes have different checkTypes slices len expected: %d", len(n.checkTypes))
	}
	for i := 0; i < len(n.checkTypes); i++ {
		if n.checkTypes[i] != n2f.checkTypes[i] {
			return fmt.Errorf("nodes have different data in checkTypes expected: %s on position", n.checkTypes)
		}
	}
	return nil
}
