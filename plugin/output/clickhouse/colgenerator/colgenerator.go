package main

import (
	_ "embed"
	"fmt"
	"html/template"
	"os"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/logger"
)

const (
	outputFileName = "column_gen.go"
)

//go:embed insane_column.go.tmpl
var columnTemplateRaw string

type Type struct {
	chTypeName string
	// Go name of the type, e.g. int8, string
	GoName string
	// Can not cast to Go type
	CannotConvert bool
	CannotBeNull  bool
	// integers with 128-256 bits
	IsComplexNumber bool
}

func (t Type) ChTypeName() string {
	return t.chTypeName
}

func (t Type) LibChTypeName() string {
	if t.ChTypeName() == string(proto.ColumnTypeString) {
		// 'String' named as 'Str' in the ch-go library
		return "Str"
	}
	return t.ChTypeName()
}

func (t Type) LibChTypeNameFull() string {
	return "proto.Col" + t.LibChTypeName()
}

func (t Type) LibChTypeNullableNameFull() string {
	return fmt.Sprintf("proto.ColNullable[%s]", t.GoName)
}

func (t Type) ColumnTypeName() string {
	return "Col" + t.ChTypeName()
}

func (t Type) InsaneConvertFunc() string {
	switch t.GoName {
	case "bool":
		return "AsBool"
	case "string":
		return "AsString"
	default:
		return "AsInt"
	}
}

type TemplateData struct {
	Types []Type
}

func main() {
	columnTemplate := template.Must(template.New("column").Parse(columnTemplateRaw))

	f, err := os.Create(outputFileName)
	if err != nil {
		logger.Fatal(err)
	}
	defer f.Close()

	types := clickhouseTypes()

	if err := columnTemplate.Execute(f, TemplateData{Types: types}); err != nil {
		logger.Fatal(err)
	}

	logger.Info("done")
}

func clickhouseTypes() []Type {
	types := []Type{
		{
			chTypeName:    "Bool",
			GoName:        "bool",
			CannotConvert: true,
		},
		{
			chTypeName:    "String",
			GoName:        "string",
			CannotConvert: true,
		},
		{
			chTypeName:   "Enum8",
			GoName:       "proto.Enum8",
			CannotBeNull: true,
		},
		{
			chTypeName:   "Enum16",
			GoName:       "proto.Enum16",
			CannotBeNull: true,
		},
	}

	for _, bits := range []int{8, 16, 32, 64} {
		for _, signed := range []bool{false, true} {
			goName := fmt.Sprintf("int%d", bits)
			protoName := fmt.Sprintf("Int%d", bits)
			if signed {
				goName = "u" + goName
				protoName = "U" + protoName
			}
			types = append(types, Type{
				chTypeName: protoName,
				GoName:     goName,
			})
		}
	}

	types = append(types,
		Type{
			chTypeName:      "Int128",
			GoName:          "proto.Int128",
			IsComplexNumber: true,
		},
		Type{
			chTypeName:      "UInt128",
			GoName:          "proto.UInt128",
			IsComplexNumber: true,
		},
		Type{
			chTypeName:      "Int256",
			GoName:          "proto.Int256",
			IsComplexNumber: true,
		},
		Type{
			chTypeName:      "UInt256",
			GoName:          "proto.UInt256",
			IsComplexNumber: true,
		},
		Type{
			chTypeName: "Float32",
			GoName:     "float32",
		},
		Type{
			chTypeName: "Float64",
			GoName:     "float64",
		},
	)

	return types
}
