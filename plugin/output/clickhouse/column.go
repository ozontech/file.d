package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	insaneJSON "github.com/vitkovskii/insane-json"
)

//go:generate go run ./colgenerator

type InsaneColumn struct {
	Name     string
	ColInput InsaneColInput
}

type InsaneColInput interface {
	proto.ColInput
	Append(node *insaneJSON.StrictNode) error
	Reset()
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
			col.ColInput = ColStr{auto.Data.(*proto.ColStr)}
		case proto.ColumnTypeInt8:
			col.ColInput = ColInt8{auto.Data.(*proto.ColInt8)}
		case proto.ColumnTypeInt16:
			col.ColInput = ColInt16{auto.Data.(*proto.ColInt16)}
		case proto.ColumnTypeEnum8, proto.ColumnTypeEnum16:
			col.ColInput = ColEnum{auto.Data.(*proto.ColEnum)}
		default:
			panic("unimplemented")
		}
		columns = append(columns, col)
	}

	return columns, nil
}
