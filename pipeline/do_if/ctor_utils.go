package do_if

import (
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
			"%w: field=%q expected=%T got=%T",
			errTypeMismatch, field, def, fieldNode,
		)
	}

	return result, nil
}

func must[T any](v any) (T, error) {
	var def T

	result, ok := v.(T)
	if !ok {
		return def, fmt.Errorf("%w: expected=%T got=%T", errTypeMismatch, def, v)
	}

	return result, nil
}
