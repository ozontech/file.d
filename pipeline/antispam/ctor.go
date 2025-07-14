package antispam

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ozontech/file.d/pipeline/checker"
	"github.com/ozontech/file.d/pipeline/logic"
)

const (
	fieldNameOp = "op"

	fieldNameOperands = "operands"

	fieldNameData          = "data"
	fieldNameValues        = "values"
	fieldNameCaseSensitive = "case_sensitive"
)

func extractNode(node map[string]any) (Node, error) {
	opName, err := get[string](node, fieldNameOp)
	if err != nil {
		return nil, err
	}

	switch opName {
	case
		logic.AndTag,
		logic.OrTag,
		logic.NotTag:
		return extractLogicalNode(opName, node)
	case
		checker.OpEqualTag,
		checker.OpContainsTag,
		checker.OpPrefixTag,
		checker.OpSuffixTag,
		checker.OpRegexTag:
		return extractValueNode(opName, node)
	default:
		return nil, fmt.Errorf("unknown op: %s", opName)
	}
}

func extractLogicalNode(op string, node map[string]any) (Node, error) {
	rawOperands, err := get[[]any](node, fieldNameOperands)
	if err != nil {
		return nil, err
	}

	operands := make([]Node, 0)

	for _, rawOperand := range rawOperands {
		operandMap, ok := rawOperand.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(
				"logical node operand type mismatch: expected=map[string]any got=%T",
				rawOperand)
		}

		operand, err := extractNode(operandMap)
		if err != nil {
			return nil, fmt.Errorf("extract operand for logical op %q: %w", op, err)
		}

		operands = append(operands, operand)
	}

	result, err := newLogicalNode(op, operands)
	if err != nil {
		return nil, fmt.Errorf("init logical node: %w", err)
	}

	return result, nil
}

func extractValueNode(op string, node map[string]any) (Node, error) {
	dataTag, err := get[string](node, fieldNameData)
	if err != nil {
		return nil, err
	}

	caseSensitive := true
	caseSensitiveNode, err := get[bool](node, fieldNameCaseSensitive)
	if err == nil {
		caseSensitive = caseSensitiveNode
	} else if errors.Is(err, errTypeMismatch) {
		return nil, err
	}

	values, err := extractValues(node)
	if err != nil {
		return nil, fmt.Errorf("extract values: %w", err)
	}

	return newValueNode(op, caseSensitive, values, dataTag)
}

func extractValues(node map[string]any) ([][]byte, error) {
	rawValues, err := get[[]any](node, fieldNameValues)
	if err != nil {
		return nil, err
	}

	values := make([][]byte, 0, len(rawValues))
	for _, rawValue := range rawValues {
		value, ok := rawValue.(string)
		if !ok {
			return nil, fmt.Errorf("type of value is not string: %T", rawValue)
		}

		values = append(values, []byte(strings.Clone(value)))
	}

	return values, nil
}
