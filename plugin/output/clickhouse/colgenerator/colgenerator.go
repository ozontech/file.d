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

	goTypeEnum = "proto.Enum"
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
			ChTypeName:    "Bool",
			GoName:        "bool",
			CannotConvert: true,
		},
		{
			ChTypeName:     "String",
			GoName:         "string",
			CannotConvert:  true,
			CustomImpl:     true,
			LowCardinality: true,
		},
		{
			ChTypeName:   "Enum8",
			GoName:       goTypeEnum,
			CannotBeNull: true,
			CustomImpl:   true,
		},
		{
			ChTypeName:   "Enum16",
			GoName:       goTypeEnum,
			CannotBeNull: true,
			CustomImpl:   true,
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
				ChTypeName: protoName,
				GoName:     goName,
			})
		}
	}

	types = append(types,
		Type{
			ChTypeName:      "Int128",
			GoName:          "proto.Int128",
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "UInt128",
			GoName:          "proto.UInt128",
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "Int256",
			GoName:          "proto.Int256",
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "UInt256",
			GoName:          "proto.UInt256",
			isComplexNumber: true,
		},
		Type{
			ChTypeName: "Float32",
			GoName:     "float32",
			CustomImpl: true,
		},
		Type{
			ChTypeName: "Float64",
			GoName:     "float64",
			CustomImpl: true,
		},
		Type{
			ChTypeName:   "DateTime",
			GoName:       "time.Time",
			CannotBeNull: true,
			CustomImpl:   true,
		},
		Type{
			ChTypeName:   "DateTime64",
			GoName:       "time.Time",
			CannotBeNull: true,
			CustomImpl:   true,
		},
		Type{
			ChTypeName: "IPv4",
			GoName:     "proto.IPv4",
			CustomImpl: true,
		},
		Type{
			ChTypeName: "IPv6",
			GoName:     "proto.IPv6",
			CustomImpl: true,
		},
	)

	return types
}
