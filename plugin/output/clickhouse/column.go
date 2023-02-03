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
	var columns []InsaneColumn
	for _, col := range schema.Columns {
		if col.Type == "" {
			return nil, fmt.Errorf("empty column type")
		}

		auto := proto.ColAuto{}
		if err := auto.Infer(proto.ColumnType(col.Type)); err != nil {
			return nil, fmt.Errorf("inref: %w", err)
		}

		col := InsaneColumn{
			Name: col.Name,
		}
		switch auto.Data.Type() {
		case proto.ColumnTypeString:
			col.ColInput = NewColStr(false)
		case proto.ColumnTypeInt8:
			col.ColInput = NewColInt8(false)
		case proto.ColumnTypeInt16:
			col.ColInput = NewColInt16(false)
		case proto.ColumnTypeEnum8, proto.ColumnTypeEnum16:
			col.ColInput = NewColEnum(false)
		default:
			panic("unimplemented")
		}
		columns = append(columns, col)
	}

	return columns, nil
}
