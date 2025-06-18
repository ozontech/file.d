package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

type ColMapStringString struct {
	*proto.ColMap[string, string]
}

func NewColMapStringString() *ColMapStringString {
	return &ColMapStringString{
		ColMap: proto.NewMap(new(proto.ColStr), new(proto.ColStr)),
	}
}

var _ InsaneColInput = (*ColMapStringString)(nil)

func (t *ColMapStringString) Append(node InsaneNode) error {
	var m map[string]string
	if node != nil {
		var err error
		m, err = node.AsMapStringString()
		if err != nil {
			return fmt.Errorf("converting node to the map of (string,string): %w", err)
		}
	}

	t.ColMap.Append(m)

	return nil
}
