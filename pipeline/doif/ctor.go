package doif

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ozontech/file.d/logger"
)

var (
	doIfLogicalOpNodes = map[string]struct{}{
		"and": {},
		"not": {},
		"or":  {},
	}
	doIfFieldOpNodes = map[string]struct{}{
		"equal":    {},
		"contains": {},
		"prefix":   {},
		"suffix":   {},
		"regex":    {},
	}
	doIfLengthCmpOpNodes = map[string]struct{}{
		"byte_len_cmp":  {},
		"array_len_cmp": {},
	}
	doIfTimestampCmpOpNodes = map[string]struct{}{
		"ts_cmp": {},
	}
	doIfCheckTypeOpNode = "check_type"
)

func NewFromMap(m map[string]any, isRawJSON bool) (*Checker, error) {
	root, err := extractDoIfNode(m, isRawJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to extract nodes: %w", err)
	}

	return &Checker{
		root: root,
	}, nil
}

func extractFieldOpVals(jsonNode map[string]any) [][]byte {
	valuesRaw, has := jsonNode["values"]
	if !has {
		return nil
	}

	switch values := valuesRaw.(type) {
	case nil:
		return [][]byte{nil}
	case string:
		return [][]byte{[]byte(values)}
	case []any:
		return extractFieldOpValsArrAny(values)
	default:
		logger.Panicf(`unknown type of field "values": %T; value %v`, values, values)
	}

	panic("unreachable")
}

func extractFieldOpValsArrAny(values []any) [][]byte {
	var vals [][]byte

	for _, value := range values {
		if value == nil {
			vals = append(vals, nil)
		} else if valueStr, ok := value.(string); ok {
			vals = append(vals, []byte(valueStr))
		} else {
			logger.Panicf(
				`elem of array "values" type mismatch; expected string or nil; got %T; value: %v`,
				value, value)
		}
	}

	return vals
}

func extractFieldOpNode(opName string, jsonNode map[string]any) (Node, error) {
	var result Node
	var err error

	fieldPath, err := requireField[string](jsonNode, "field")
	if err != nil {
		return nil, err
	}

	caseSensitive := true
	caseSensitiveNode, err := requireField[bool](jsonNode, "case_sensitive")
	if err == nil {
		caseSensitive = caseSensitiveNode
	} else if errors.Is(err, errFieldTypeMismatch) {
		return nil, err
	}

	vals := extractFieldOpVals(jsonNode)
	result, err = NewFieldOpNode(opName, fieldPath, caseSensitive, vals)
	if err != nil {
		return nil, fmt.Errorf("failed to init field op: %w", err)
	}

	return result, nil
}

var errFieldNotFound = errors.New("field not found")

func fieldNotFoundError(field string) error {
	return fmt.Errorf("%w: %s", errFieldNotFound, field)
}

var errFieldTypeMismatch = errors.New("field type mismatch")

func must[T any](jsonNode map[string]any, fieldName string) (T, error) {
	var def T

	node, has := jsonNode[fieldName]
	if !has {
		return def, fieldNotFoundError(fieldName)
	}

	result, ok := node.(T)
	if !ok {
		return def, fmt.Errorf(
			"%w; field %q; type %T; value: %v",
			errFieldTypeMismatch, fieldName, result, result,
		)
	}

	return result, nil
}

func requireField[T any](jsonNode map[string]any, fieldName string) (T, error) {
	var def T

	res, err := must[T](jsonNode, fieldName)
	if err != nil {
		return def, err
	}

	return res, nil
}

const (
	fieldNameField    = "field"
	fieldNameCmpOp    = "cmp_op"
	fieldNameCmpValue = "value"
)

func extractLengthCmpOpNode(opName string, jsonNode map[string]any, isRawJSON bool) (Node, error) {
	fieldPath, err := requireField[string](jsonNode, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := requireField[string](jsonNode, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	cmpValue, err := extractLengthCmpVal(jsonNode, isRawJSON)
	if err != nil {
		return nil, err
	}

	return NewLenCmpOpNode(opName, fieldPath, cmpOp, cmpValue)
}

func extractLengthCmpVal(jsonNode map[string]any, isRawJSON bool) (int, error) {
	if isRawJSON {
		cmpValueWrapped, err := requireField[json.Number](jsonNode, fieldNameCmpValue)
		if err != nil {
			return 0, err
		}

		cmpValue, err := cmpValueWrapped.Int64()
		if err != nil {
			return 0, err
		}

		return int(cmpValue), nil
	}

	cmpValue, err := requireField[float64](jsonNode, fieldNameCmpValue)
	if err != nil {
		return 0, err
	}

	return int(cmpValue), nil
}

const (
	fieldNameFormat         = "format"
	fieldNameUpdateInterval = "update_interval"
	fieldNameCmpValueShift  = "value_shift"
)

const (
	tsCmpModeNowTag   = "now"
	tsCmpModeConstTag = "const"

	tsCmpValueNowTag   = "now"
	tsCmpValueStartTag = "file_d_start"
)

const (
	defaultTsCmpValUpdateInterval = 10 * time.Second
	defaultTsFormat               = "rfc3339nano"
)

func extractTsCmpOpNode(_ string, jsonNode map[string]any) (Node, error) {
	fieldPath, err := requireField[string](jsonNode, fieldNameField)
	if err != nil {
		return nil, err
	}

	cmpOp, err := requireField[string](jsonNode, fieldNameCmpOp)
	if err != nil {
		return nil, err
	}

	rawCmpValue, err := requireField[string](jsonNode, fieldNameCmpValue)
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
	str, err := requireField[string](jsonNode, fieldNameFormat)
	if err == nil {
		format = str
	} else if errors.Is(err, errFieldTypeMismatch) {
		return nil, err
	}

	cmpValueShift := time.Duration(0)
	str, err = requireField[string](jsonNode, fieldNameCmpValueShift)
	if err == nil {
		cmpValueShift, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse cmp value shift: %w", err)
		}
	} else if errors.Is(err, errFieldTypeMismatch) {
		return nil, err
	}

	updateInterval := defaultTsCmpValUpdateInterval
	str, err = requireField[string](jsonNode, fieldNameUpdateInterval)
	if str != "" {
		updateInterval, err = time.ParseDuration(str)
		if err != nil {
			return nil, fmt.Errorf("parse update interval: %w", err)
		}
	} else if errors.Is(err, errFieldTypeMismatch) {
		return nil, err
	}

	return NewTsCmpOpNode(fieldPath, format, cmpOp, cmpMode, cmpValue, cmpValueShift, updateInterval)
}

func extractCheckTypeOpNode(_ string, jsonNode map[string]any) (Node, error) {
	fieldPath, err := requireField[string](jsonNode, "field")
	if err != nil {
		return nil, err
	}
	vals := extractFieldOpVals(jsonNode)
	result, err := NewCheckTypeOpNode(fieldPath, vals)
	if err != nil {
		return nil, fmt.Errorf("failed to init check_type op: %w", err)
	}
	return result, nil
}

func extractLogicalOpNode(opName string, jsonNode map[string]any, isRawJSON bool) (Node, error) {
	var result, operand Node
	var err error

	rawOperands, err := requireField[[]any](jsonNode, "operands")
	if err != nil {
		return nil, err
	}

	operandsList := make([]Node, 0)

	for _, rawOperand := range rawOperands {
		operandMap, ok := rawOperand.(map[string]any)
		if !ok {
			return nil,
				fmt.Errorf(
					"logical op type mismatch; expected map[string]any; got: %T; value: %v",
					operandMap, operandMap,
				)
		}

		operand, err = extractDoIfNode(operandMap, isRawJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to extract operand node for logical op %q", opName)
		}
		operandsList = append(operandsList, operand)
	}

	result, err = NewLogicalNode(opName, operandsList)
	if err != nil {
		return nil, fmt.Errorf("failed to init logical node: %w", err)
	}

	return result, nil
}

func extractDoIfNode(jsonNode map[string]any, isRawJSON bool) (Node, error) {
	opName, err := requireField[string](jsonNode, "op")
	if err != nil {
		return nil, err
	}

	if _, has := doIfLogicalOpNodes[opName]; has {
		return extractLogicalOpNode(opName, jsonNode, isRawJSON)
	} else if _, has := doIfFieldOpNodes[opName]; has {
		return extractFieldOpNode(opName, jsonNode)
	} else if _, has := doIfLengthCmpOpNodes[opName]; has {
		return extractLengthCmpOpNode(opName, jsonNode, isRawJSON)
	} else if _, has := doIfTimestampCmpOpNodes[opName]; has {
		return extractTsCmpOpNode(opName, jsonNode)
	} else if opName == doIfCheckTypeOpNode {
		return extractCheckTypeOpNode(opName, jsonNode)
	} else {
		return nil, fmt.Errorf("unknown op %q", opName)
	}
}
