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

func inferInsaneColInputs(columns []Column) ([]InsaneColumn, error) {
	insaneColumns := make([]InsaneColumn, 0, len(columns))
	for _, col := range columns {
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

		insaneColumns = append(insaneColumns, InsaneColumn{
			Name:     col.Name,
			ColInput: insaneCol,
		})
	}

	return insaneColumns, nil
}
