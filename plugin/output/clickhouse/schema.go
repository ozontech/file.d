package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Schema struct {
	Columns []Column `json:"columns"`
}

func parseSchema(schema Schema) (proto.Input, error) {
	var fields proto.Input
	for _, col := range schema.Columns {
		if col.Type == "" {
			return nil, fmt.Errorf("empty column type")
		}

		auto := proto.ColAuto{}
		if err := auto.Infer(proto.ColumnType(col.Type)); err != nil {
			return nil, fmt.Errorf("inref: %w", err)
		}

		fields = append(fields, proto.InputColumn{
			Name: col.Name,
			Data: auto.Data,
		})
	}

	return fields, nil
}
