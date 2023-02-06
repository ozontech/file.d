package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

//go:generate go run ./colgenerator

type InsaneColumn struct {
	Name     string
	ColInput InsaneColInput
}

func inferInsaneColInputs(schema Schema) ([]InsaneColumn, error) {
	columns := make([]InsaneColumn, 0, len(schema.Columns))
	for _, col := range schema.Columns {
		if col.Type == "" {
			return nil, fmt.Errorf("empty column type")
		}

		auto := proto.ColAuto{}
		if err := auto.Infer(proto.ColumnType(col.Type)); err != nil {
			return nil, fmt.Errorf("infer: %w", err)
		}

		insaneCol, err := insaneInfer(auto)
		if err != nil {
			return nil, err
		}

		columns = append(columns, InsaneColumn{
			Name:     col.Name,
			ColInput: insaneCol,
		})
	}

	return columns, nil
}
