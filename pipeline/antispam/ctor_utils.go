package antispam

import (
	"encoding/json"
	"errors"
	"fmt"
)

var (
	errFieldNotFound = errors.New("field not found")
	errTypeMismatch  = errors.New("type mismatch")
)

func getAny(node map[string]any, field string) (any, error) {
	res, has := node[field]
	if !has {
		return nil, fmt.Errorf("field=%q: %w", field, errFieldNotFound)
	}

	return res, nil
}

func get[T any](node map[string]any, field string) (T, error) {
	var def T

	fieldNode, err := getAny(node, field)
	if err != nil {
		return def, err
	}

	result, ok := fieldNode.(T)
	if !ok {
		return def, fmt.Errorf(
			"field=%q expected=%T got=%T: %w",
			field, def, fieldNode, errTypeMismatch,
		)
	}

	return result, nil
}

func anyToInt(v any) (int, error) {
	switch vNum := v.(type) {
	case int:
		return vNum, nil
	case float64:
		return int(vNum), nil
	case json.Number:
		vInt64, err := vNum.Int64()
		if err != nil {
			return 0, err
		}
		return int(vInt64), nil
	default:
		return 0, fmt.Errorf("type=%T not convertable to int", v)
	}
}
