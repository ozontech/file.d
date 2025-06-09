package mask

import (
	"fmt"
	"strings"
	"time"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline/doif"
)

type TreeNode struct {
	// common fields
	Op    string `json:"op"`
	Field string `json:"field"`

	// field-op or check-type-op
	Values any `json:"values"`

	// field-op
	CaseSensitive *bool `json:"case_sensitive"`

	// logical-op
	Operands []*TreeNode `json:"operands"`

	// len-cmp-op or ts-cmp-op
	Value any    `json:"value"`
	CmpOp string `json:"cmp_op"`

	// ts-cmp-op
	TsFormat         string        `json:"format"`
	TsValueShift     time.Duration `json:"value_shift"`
	TsUpdateInterval time.Duration `json:"update_interval"`
}

func extractFieldOpVals(values any) [][]byte {
	if values == nil {
		return nil
	}

	vals := make([][]byte, 0)

	valStr, ok := values.(string)
	if ok {
		vals = append(vals, []byte(strings.Clone(valStr)))
		return vals
	}

	arrAny, ok := values.([]any)
	if ok {
		for _, elem := range arrAny {
			if elem == nil {
				vals = append(vals, nil)
			} else {
				vals = append(vals, []byte(strings.Clone(elem.(string))))
			}
		}
		return vals
	}

	arrStr, ok := values.([]string)
	if ok {
		for _, elem := range arrStr {
			vals = append(vals, []byte(strings.Clone(elem)))
		}
		return vals
	}

	panic(`unknown type of field "values"`)
}

func extractFieldOpNode(opName string, node *TreeNode) (doif.Node, error) {
	caseSensitive := true
	if node.CaseSensitive != nil {
		caseSensitive = *node.CaseSensitive
	}

	vals := extractFieldOpVals(node.Values)

	result, err := doif.NewFieldOpNode(opName, node.Field, caseSensitive, vals)
	if err != nil {
		return nil, fmt.Errorf("init field op: %w", err)
	}

	return result, nil
}

func extractLengthCmpOpNode(opName string, node *TreeNode) (doif.Node, error) {
	cmpValue, ok := node.Value.(int)
	if !ok {
		return nil, fmt.Errorf(`"value" field type is not int`)
	}

	return doif.NewLenCmpOpNode(opName, node.Field, node.CmpOp, cmpValue)
}

func extractCheckTypeOpNode(_ string, node *TreeNode) (doif.Node, error) {
	vals := extractFieldOpVals(node.Values)

	result, err := doif.NewCheckTypeOpNode(node.Field, vals)
	if err != nil {
		return nil, fmt.Errorf("init check_type op: %w", err)
	}

	return result, nil
}

func extractLogicalOpNode(opName string, node *TreeNode) (doif.Node, error) {
	var result, operand doif.Node
	var err error

	operandsList := make([]doif.Node, 0, len(node.Operands))
	for _, operandNode := range node.Operands {
		operand, err = extractDoIfNode(operandNode)
		if err != nil {
			return nil, fmt.Errorf("extract operand node for logical op %q", opName)
		}

		operandsList = append(operandsList, operand)
	}

	result, err = doif.NewLogicalNode(opName, operandsList)
	if err != nil {
		return nil, fmt.Errorf("init logical node: %w", err)
	}

	return result, nil
}

func extractDoIfNode(node *TreeNode) (doif.Node, error) {
	op := node.Op

	if _, has := fd.DoIfLogicalOpNodes[op]; has {
		return extractLogicalOpNode(op, node)
	}
	if _, has := fd.DoIfFieldOpNodes[op]; has {
		return extractFieldOpNode(op, node)
	}
	if _, has := fd.DoIfLengthCmpOpNodes[op]; has {
		return extractLengthCmpOpNode(op, node)
	}
	if op == fd.DoIfCheckTypeOpNode {
		return extractCheckTypeOpNode(op, node)
	}

	return nil, fmt.Errorf("unknown op %q", op)
}
