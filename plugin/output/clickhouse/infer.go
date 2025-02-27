package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

//go:generate go run ./colgenerator

type InsaneColInput interface {
	proto.ColInput
	Append(node InsaneNode) error
	Reset()
}

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
			return nil, fmt.Errorf("auto infer: %w", err)
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

func insaneInfer(auto proto.ColAuto) (InsaneColInput, error) {
	parent := auto.Type().Base()
	nullable := parent == proto.ColumnTypeNullable
	lowCardinality := parent == proto.ColumnTypeLowCardinality

	child := auto.Type()
	if nullable || lowCardinality {
		// trim "Nullable()", "LowCardinality()"
		child = child.Elem()
	}

	switch parent {
	case proto.ColumnTypeEnum8:
		return NewColEnum8(auto.Data.(*proto.ColEnum)), nil
	case proto.ColumnTypeEnum16:
		return NewColEnum16(auto.Data.(*proto.ColEnum)), nil
	case proto.ColumnTypeDateTime:
		return NewColDateTime(auto.Data.(*proto.ColDateTime)), nil
	case proto.ColumnTypeDateTime64:
		col := auto.Data.(*proto.ColDateTime64)
		return NewColDateTime64(col, col.Precision), nil
	case proto.ColumnTypeArray:
		child := child.Elem()
		if child == proto.ColumnTypeString {
			return NewColStringArray(), nil
		}
		return nil, fmt.Errorf("array of type %q is not supported", child.String())
	default:
		switch child {
		case proto.ColumnTypeBool:
			return NewColBool(nullable), nil
		case proto.ColumnTypeString:
			return NewColString(nullable, lowCardinality), nil
		case proto.ColumnTypeInt8:
			return NewColInt8(nullable), nil
		case proto.ColumnTypeUInt8:
			return NewColUInt8(nullable), nil
		case proto.ColumnTypeInt16:
			return NewColInt16(nullable), nil
		case proto.ColumnTypeUInt16:
			return NewColUInt16(nullable), nil
		case proto.ColumnTypeInt32:
			return NewColInt32(nullable), nil
		case proto.ColumnTypeUInt32:
			return NewColUInt32(nullable), nil
		case proto.ColumnTypeInt64:
			return NewColInt64(nullable), nil
		case proto.ColumnTypeUInt64:
			return NewColUInt64(nullable), nil
		case proto.ColumnTypeInt128:
			return NewColInt128(nullable), nil
		case proto.ColumnTypeUInt128:
			return NewColUInt128(nullable), nil
		case proto.ColumnTypeInt256:
			return NewColInt256(nullable), nil
		case proto.ColumnTypeUInt256:
			return NewColUInt256(nullable), nil
		case proto.ColumnTypeFloat32:
			return NewColFloat32(nullable), nil
		case proto.ColumnTypeFloat64:
			return NewColFloat64(nullable), nil
		case proto.ColumnTypeDateTime:
			return NewColDateTime(auto.Data.(*proto.ColDateTime)), nil
		case proto.ColumnTypeIPv4:
			return NewColIPv4(nullable), nil
		case proto.ColumnTypeIPv6:
			return NewColIPv6(nullable), nil
		case proto.ColumnTypeUUID:
			return NewColUUID(nullable), nil
		default:
			return nil, fmt.Errorf("inference for type %q is not supported", auto.Type().String())
		}
	}
}

func inputFromColumns(cols []InsaneColumn) proto.Input {
	input := make(proto.Input, len(cols))
	for i := range cols {
		input[i] = proto.InputColumn{
			Name: cols[i].Name,
			Data: cols[i].ColInput,
		}
	}
	return input
}
