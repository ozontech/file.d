package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

type ColStringArray struct {
	*proto.ColArr[string]
}

func NewColStringArray() *ColStringArray {
	return &ColStringArray{
		ColArr: new(proto.ColStr).Array(),
	}
}

var _ InsaneColInput = (*ColStringArray)(nil)

func (t *ColStringArray) Append(array InsaneNode) error {
	if array == nil || array.IsNull() {
		t.ColArr.Append([]string{})
		return nil
	}

	if !array.IsArray() {
		t.ColArr.Append([]string{array.EncodeToString()})
		return nil
	}

	vals, err := array.AsStringArray()
	if err != nil {
		return fmt.Errorf("converting node to the string array: %w", err)
	}

	t.ColArr.Append(vals)

	return nil
}
