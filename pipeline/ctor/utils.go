package ctor

import (
	"errors"
	"fmt"
)

type Node = map[string]any

var (
	ErrFieldNotFound = errors.New("field not found")
	ErrTypeMismatch  = errors.New("type mismatch")
)

func GetAny(node Node, field string) (any, error) {
	res, has := node[field]
	if !has {
		return nil, fmt.Errorf("field=%q: %w", field, ErrFieldNotFound)
	}

	return res, nil
}

func Must[T any](v any) (T, error) {
	var def T

	result, ok := v.(T)
	if !ok {
		return def, fmt.Errorf("%w: expected=%T got=%T", ErrTypeMismatch, def, v)
	}

	return result, nil
}

func Get[T any](node Node, field string, defValues ...T) (T, error) {
	if len(defValues) > 1 {
		panic("too many default values")
	}

	var def T

	fieldNode, err := GetAny(node, field)
	if err != nil {
		if len(defValues) == 1 {
			return defValues[0], nil
		}

		return def, err
	}

	result, ok := fieldNode.(T)
	if !ok {
		return def, fmt.Errorf(
			"%w: field=%q expected=%T got=%T",
			ErrTypeMismatch, field, def, fieldNode,
		)
	}

	return result, nil
}
