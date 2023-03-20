package main

import (
	"bytes"
	_ "embed"
	"fmt"
	"go/format"
	"os"
	"text/template"

	"github.com/ozontech/file.d/logger"
)

const (
	outputFileName = "column_gen.go"

	goTypeTime = "time.Time"
	goTypeEnum = "proto.Enum"
	goTypeIPv4 = "proto.IPv4"
	goTypeIPv6 = "proto.IPv6"
)

//go:embed insane_column.go.tmpl
var columnTemplateRaw string

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
		fmt.Println(buf.String())
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
		Type{
			chTypeName:   "DateTime64",
			GoName:       goTypeTime,
			CannotBeNull: true,
		},
		Type{
			chTypeName: "IPv4",
			GoName:     goTypeIPv4,
		},
		Type{
			chTypeName: "IPv6",
			GoName:     goTypeIPv6,
		},
	)

	return types
}
