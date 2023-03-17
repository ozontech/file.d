package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"os"
	"strings"
	"text/template"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/logger"
)

const (
	outputFileName = "column_gen.go"

	goTypeTime = "time.Time"
	goTypeEnum = "proto.Enum"
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
	isComplexNumber bool
}

func (t Type) ChTypeName() string {
	return t.chTypeName
}

func (t Type) LibChTypeName() string {
	if t.ChTypeName() == string(proto.ColumnTypeString) {
		// 'String' named as 'Str' in the ch-go library
		return "Str"
	}
	if t.GoName == goTypeEnum {
		return "Enum"
	}
	return t.ChTypeName()
}

func (t Type) LibChTypeNameFull() string {
	return "proto.Col" + t.LibChTypeName()
}

func (t Type) NullableTypeName() string {
	return fmt.Sprintf("proto.ColNullable[%s]", t.GoName)
}

func (t Type) ColumnTypeName() string {
	return "Col" + t.ChTypeName()
}

func (t Type) InsaneConvertFunc() string {
	switch t.GoName {
	case "bool":
		return "AsBool"
	case "string", goTypeEnum:
		return "AsString"
	case goTypeTime:
		return "AsInt"
	default:
		return "AsInt"
	}
}

type FuncArg struct {
	Name, Type string
}

func (t Type) CtorArgs() []FuncArg {
	if t.GoName == goTypeEnum {
		return []FuncArg{
			{
				Name: "col",
				Type: "*proto.ColEnum",
			},
		}
	}
	if t.CannotBeNull {
		return []FuncArg{}
	}
	return []FuncArg{
		{
			Name: "nullable",
			Type: "bool",
		},
	}
}

func (t Type) Ctor() string {
	b := strings.Builder{}
	b.WriteString(fmt.Sprintf("func New%s(", t.ColumnTypeName()))
	for i, arg := range t.CtorArgs() {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(arg.Name)
		b.WriteString(" ")
		b.WriteString(arg.Type)
	}
	b.WriteString(fmt.Sprintf(") *%s {\n", t.ColumnTypeName()))

	b.WriteString(fmt.Sprintf("\treturn &%s{\n", t.ColumnTypeName()))
	if t.CannotBeNull {
		if t.GoName == goTypeEnum {
			b.WriteString("\t\tcol: col,\n")
		} else {
			b.WriteString(fmt.Sprintf("\t\tcol: &%s{},\n", t.LibChTypeNameFull()))
		}
	} else {
		b.WriteString(fmt.Sprintf("\t\tcol:      &%s{},\n", t.LibChTypeNameFull()))
		b.WriteString(fmt.Sprintf("\t\tnullCol:  proto.NewColNullable(proto.ColumnOf[%s](&%s{})),\n", t.GoName, t.LibChTypeNameFull()))
		b.WriteString("\t\tnullable: nullable,\n")
	}
	b.WriteString("\t}\n")
	b.WriteString("}")

	return b.String()
}

func (t Type) CallCtor() string {
	var args strings.Builder
	for _, arg := range t.CtorArgs() {
		args.WriteString(arg.Name)
	}
	return fmt.Sprintf("New%s(%s)", t.ColumnTypeName(), args.String())
}

func (t Type) ConvertInsaneJSONValue(varName string) string {
	if t.isComplexNumber {
		return fmt.Sprintf("%sFromInt(%s)", t.GoName, varName)
	}
	if t.GoName == goTypeTime {
		return fmt.Sprintf("time.Unix(int64(%s), 0)", varName)
	}
	if t.GoName == goTypeEnum {
		return varName
	}
	return fmt.Sprintf("%s(%s)", t.GoName, varName)
}

func (t Type) Preparable() bool {
	return t.GoName == goTypeEnum
}

type TemplateData struct {
	Types []Type
}

func main() {
	columnTemplate := template.Must(template.New("column").Parse(columnTemplateRaw))

	types := clickhouseTypes()

	buf := new(bytes.Buffer)
	if err := columnTemplate.Execute(buf, TemplateData{Types: types}); err != nil {
		logger.Panic(err)
	}
	result, err := format.Source(buf.Bytes())
	if err != nil {
		logger.Panic(err)
	}

	f, err := os.Create(outputFileName)
	if err != nil {
		logger.Panic(err)
	}
	defer f.Close()

	_, err = f.Write(result)
	if err != nil {
		logger.Panic(err)
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
			chTypeName:    "Enum8",
			GoName:        goTypeEnum,
			CannotBeNull:  true,
			CannotConvert: true,
		},
		{
			chTypeName:    "Enum16",
			GoName:        goTypeEnum,
			CannotBeNull:  true,
			CannotConvert: true,
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
			isComplexNumber: true,
		},
		Type{
			chTypeName:      "UInt128",
			GoName:          "proto.UInt128",
			isComplexNumber: true,
		},
		Type{
			chTypeName:      "Int256",
			GoName:          "proto.Int256",
			isComplexNumber: true,
		},
		Type{
			chTypeName:      "UInt256",
			GoName:          "proto.UInt256",
			isComplexNumber: true,
		},
		Type{
			chTypeName: "Float32",
			GoName:     "float32",
		},
		Type{
			chTypeName: "Float64",
			GoName:     "float64",
		},
		Type{
			chTypeName:   "DateTime",
			GoName:       goTypeTime,
			CannotBeNull: true,
		},
	)

	return types
}
