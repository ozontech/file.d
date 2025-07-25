package do_if

import (
	"errors"
	"fmt"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline/ctor"
	"github.com/ozontech/file.d/pipeline/do_if/data_checker"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
)

const (
	fieldNameOp = "op"

	fieldNameField = "field"
	fieldNameData  = "data"

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
	root, err := ExtractNode(m)
	if err != nil {
		return nil, fmt.Errorf("extract nodes: %w", err)
	}

	return &Checker{
		root: root,
	}, nil
}

func ExtractNode(node ctor.Node) (Node, error) {
	opName, err := ctor.Get[string](node, fieldNameOp)
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
		data_checker.OpEqualTag,
		data_checker.OpContainsTag,
		data_checker.OpPrefixTag,
		data_checker.OpSuffixTag,
		data_checker.OpRegexTag:
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

	fieldPath, err := ctor.Get[string](node, fieldNameField)
	fieldPathFound := err == nil
	if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	dataTypeTag, err := ctor.Get[string](node, fieldNameData)
	dataTypeTagFound := err == nil
	if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	switch {
	case fieldPathFound && dataTypeTagFound:
		return nil, errors.New("field selector and data type tag provided")
	case !fieldPathFound && !dataTypeTagFound:
		return nil, errors.New("field selector and data type tag are not provided")
	}

	caseSensitive := true
	caseSensitiveNode, err := ctor.Get[bool](node, fieldNameCaseSensitive)
	if err == nil {
		caseSensitive = caseSensitiveNode
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	vals, err := extractOpValues(node)
	if err != nil {
		return nil, fmt.Errorf("extract field op values: %w", err)
	}

	result, err = newFieldOpNode(opName, caseSensitive, vals, fieldPath, dataTypeTag)
	if err != nil {
		return nil, fmt.Errorf("init field op: %w", err)
	}

	return result, nil
}

func extractOpValues(node map[string]any) ([][]byte, error) {
	valuesRaw, err := ctor.GetAny(node, fieldNameValues)
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
	fieldPath, err := ctor.Get[string](node, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := ctor.Get[string](node, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	cmpValueRaw, err := ctor.GetAny(node, fieldNameCmpValue)
	if err != nil {
		return nil, err
	}

	cmpValue, err := cfg.AnyToInt(cmpValueRaw)
	if err != nil {
		return nil, err
	}

	return newLenCmpOpNode(opName, fieldPath, cmpOp, cmpValue)
}

func extractTsCmpOpNode(_ string, node map[string]any) (Node, error) {
	fieldPath, err := ctor.Get[string](node, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := ctor.Get[string](node, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	rawCmpValue, err := ctor.Get[string](node, fieldNameCmpValue)
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
	str, err := ctor.Get[string](node, fieldNameFormat)
	if err == nil {
		format = str
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	cmpValueShift := time.Duration(0)
	str, err = ctor.Get[string](node, fieldNameCmpValueShift)
	if err == nil {
		cmpValueShift, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse cmp value shift: %w", err)
		}
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	updateInterval := defaultTsCmpValUpdateInterval
	str, err = ctor.Get[string](node, fieldNameUpdateInterval)
	if err == nil {
		updateInterval, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse update interval: %w", err)
		}
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	return newTsCmpOpNode(fieldPath, format, cmpOp, cmpMode, cmpValue, cmpValueShift, updateInterval)
}

func extractCheckTypeOpNode(_ string, node map[string]any) (Node, error) {
	fieldPath, err := ctor.Get[string](node, fieldNameField)
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
	rawOperands, err := ctor.Get[[]any](node, fieldNameOperands)
	if err != nil {
		return nil, err
	}

	operands := make([]Node, 0)

	for _, rawOperand := range rawOperands {
		var operandNode map[string]any
		operandNode, err = ctor.Must[map[string]any](rawOperand)
		if err != nil {
			return nil, fmt.Errorf("logical node operand type mismatch: %w", err)
		}

		var operand Node
		operand, err = ExtractNode(operandNode)
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
