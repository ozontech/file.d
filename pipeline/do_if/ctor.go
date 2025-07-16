package do_if

import (
	"errors"
	"fmt"
	"time"

	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/pipeline/checker"
	"github.com/ozontech/file.d/pipeline/logic"
)

const (
	fieldNameOp    = "op"
	fieldNameField = "field"

	fieldNameCaseSensitive = "case_sensitive"

	fieldNameValues = "values"

	fieldNameCmpOp    = "cmp_op"
	fieldNameCmpValue = "value"

	fieldNameFormat         = "format"
	fieldNameUpdateInterval = "update_interval"
	fieldNameCmpValueShift  = "value_shift"

	tsCmpModeNowTag   = "now"
	tsCmpModeConstTag = "const"

	tsCmpValueNowTag   = "now"
	tsCmpValueStartTag = "file_d_start"

	defaultTsCmpValUpdateInterval = 10 * time.Second
	defaultTsFormat               = "rfc3339nano"

	fieldNameOperands = "operands"
)

func NewFromMap(m map[string]any) (*Checker, error) {
	root, err := extractDoIfNode(m)
	if err != nil {
		return nil, fmt.Errorf("extract nodes: %w", err)
	}

	return &Checker{
		root: root,
	}, nil
}

func extractDoIfNode(node map[string]any) (Node, error) {
	opName, err := get[string](node, fieldNameOp)
	if err != nil {
		return nil, err
	}

	switch opName {
	case
		logic.AndTag,
		logic.OrTag,
		logic.NotTag:
		return extractLogicalOpNode(opName, node)
	case
		checker.OpEqualTag,
		checker.OpContainsTag,
		checker.OpPrefixTag,
		checker.OpSuffixTag,
		checker.OpRegexTag:
		return extractFieldOpNode(opName, node)
	case
		"byte_len_cmp",
		"array_len_cmp":
		return extractLengthCmpOpNode(opName, node)
	case "ts_cmp":
		return extractTsCmpOpNode(opName, node)
	case "check_type":
		return extractCheckTypeOpNode(opName, node)
	default:
		return nil, fmt.Errorf("unknown op: %s", opName)
	}
}

func extractFieldOpNode(opName string, node map[string]any) (Node, error) {
	var result Node
	var err error

	fieldPath, err := get[string](node, fieldNameField)
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

	vals, err := extractOpValues(node)
	if err != nil {
		return nil, fmt.Errorf("extract field op values: %w", err)
	}

	result, err = newFieldOpNode(opName, fieldPath, caseSensitive, vals)
	if err != nil {
		return nil, fmt.Errorf("init field op: %w", err)
	}

	return result, nil
}

func extractOpValues(node map[string]any) ([][]byte, error) {
	valuesRaw, err := getAny(node, fieldNameValues)
	if err != nil {
		return nil, err
	}

	switch values := valuesRaw.(type) {
	case nil:
		return [][]byte{nil}, nil
	case string:
		return [][]byte{[]byte(values)}, nil
	case []any:
		return extractOpValuesFromArr(values)
	default:
		return nil, fmt.Errorf("type=%T not convertable to op values", values)
	}
}

func extractOpValuesFromArr(values []any) ([][]byte, error) {
	var vals [][]byte

	for _, value := range values {
		switch v := value.(type) {
		case nil:
			vals = append(vals, nil)
		case string:
			vals = append(vals, []byte(v))
		default:
			return nil, fmt.Errorf("type=%T not convertable to one op value", v)
		}
	}

	return vals, nil
}

func extractLengthCmpOpNode(opName string, node map[string]any) (Node, error) {
	fieldPath, err := get[string](node, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := get[string](node, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	cmpValueRaw, err := getAny(node, fieldNameCmpValue)
	if err != nil {
		return nil, err
	}

	cmpValue, err := decoder.AnyToInt(cmpValueRaw)
	if err != nil {
		return nil, err
	}

	return newLenCmpOpNode(opName, fieldPath, cmpOp, cmpValue)
}

func extractTsCmpOpNode(_ string, node map[string]any) (Node, error) {
	fieldPath, err := get[string](node, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := get[string](node, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	rawCmpValue, err := get[string](node, fieldNameCmpValue)
	if err != nil {
		return nil, err
	}

	var cmpMode string
	var cmpValue time.Time

	switch rawCmpValue {
	case tsCmpValueNowTag:
		cmpMode = tsCmpModeNowTag
	case tsCmpValueStartTag:
		cmpMode = tsCmpModeConstTag
		cmpValue = time.Now()
	default:
		cmpMode = tsCmpModeConstTag
		cmpValue, err = time.Parse(time.RFC3339Nano, rawCmpValue)
		if err != nil {
			return nil, fmt.Errorf("parse ts cmp value: %w", err)
		}
	}

	format := defaultTsFormat
	str, err := get[string](node, fieldNameFormat)
	if err == nil {
		format = str
	} else if errors.Is(err, errTypeMismatch) {
		return nil, err
	}

	cmpValueShift := time.Duration(0)
	str, err = get[string](node, fieldNameCmpValueShift)
	if err == nil {
		cmpValueShift, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse cmp value shift: %w", err)
		}
	} else if errors.Is(err, errTypeMismatch) {
		return nil, err
	}

	updateInterval := defaultTsCmpValUpdateInterval
	str, err = get[string](node, fieldNameUpdateInterval)
	if err == nil {
		updateInterval, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse update interval: %w", err)
		}
	} else if errors.Is(err, errTypeMismatch) {
		return nil, err
	}

	return newTsCmpOpNode(fieldPath, format, cmpOp, cmpMode, cmpValue, cmpValueShift, updateInterval)
}

func extractCheckTypeOpNode(_ string, node map[string]any) (Node, error) {
	fieldPath, err := get[string](node, fieldNameField)
	if err != nil {
		return nil, err
	}

	vals, err := extractOpValues(node)
	if err != nil {
		return nil, fmt.Errorf("extract check type op values: %w", err)
	}

	result, err := newCheckTypeOpNode(fieldPath, vals)
	if err != nil {
		return nil, fmt.Errorf("init check_type op: %w", err)
	}

	return result, nil
}

func extractLogicalOpNode(opName string, node map[string]any) (Node, error) {
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

		operand, err := extractDoIfNode(operandMap)
		if err != nil {
			return nil, fmt.Errorf("extract operand for logical op %q: %w", opName, err)
		}
		operands = append(operands, operand)
	}

	result, err := newLogicalNode(opName, operands)
	if err != nil {
		return nil, fmt.Errorf("init logical node: %w", err)
	}

	return result, nil
}
