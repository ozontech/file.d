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

	fieldNameData          = "data"
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
	panic("not impl")
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
	rawValues, err := get[[]any](node, "values")
	if err != nil {
		return nil, err
	}

	values := make([][]byte, 0, len(rawValues))
	for _, rawValue := range rawValues {
		value, ok := rawValue.(string)
		if !ok {
			return nil, fmt.Errorf("type=%T is not string", rawValue)
		}

		values = append(values, []byte(strings.Clone(value)))
	}

	return values, nil
}
