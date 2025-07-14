package antispam

import (
	"encoding/json"
	"fmt"
)

func getAny(node map[string]any, field string) any {
	res, has := node[field]
	if !has {
		panic(fmt.Sprintf("field %q not found", field))
	}

	return res
}

func get[T any](node map[string]any, field string) T {
	fieldNode := getAny(node, field)

	result, ok := fieldNode.(T)
	if !ok {
		panic(fmt.Sprintf("field %q type mismatch: expected=%T got=%T", field, *new(T), fieldNode))
	}

	return result
}

func anyToInt(v any) int {
	switch vNum := v.(type) {
	case int:
		return vNum
	case float64:
		return int(vNum)
	case json.Number:
		vInt64, err := vNum.Int64()
		if err != nil {
			panic(err.Error())
		}
		return int(vInt64)
	default:
		panic(fmt.Sprintf("type=%T not convertable to int", v))
	}
}
