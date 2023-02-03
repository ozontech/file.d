package main

import (
	_ "embed"
	"html/template"
	"os"

	"github.com/ozontech/file.d/logger"
)

const (
	outputFileName = "column_gen.go"
)

//go:embed insane_column.go.tmpl
var columnTemplateRaw string

type Type struct {
	ProtoName string
	// 'String' has 'Str' alias in the ch-go library
	ProtoAlias string
	// insaneJSON.node's encode function
	InsaneConvertFunc string
	// Go name of the type, e.g. int8, string
	GoName string
	// Can not cast to Go type
	CannotConvert bool
	CannotBeNull  bool
	// integers with 128-256 bits
	IsComplexNumber bool
}

type TemplateData struct {
	Types []Type
}

func main() {
	columnTemplate := template.Must(template.New("column").Parse(columnTemplateRaw))

	data := TemplateData{Types: []Type{
		{
			ProtoName:         "Str",
			ProtoAlias:        "String",
			InsaneConvertFunc: "AsString",
			GoName:            "string",
			CannotConvert:     true,
		},
		{
			ProtoName:         "Enum8",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.Enum8",
			CannotBeNull:      true,
		},
		{
			ProtoName:         "Enum16",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.Enum16",
			CannotBeNull:      true,
		},
		{
			ProtoName:         "Int8",
			InsaneConvertFunc: "AsInt",
			GoName:            "int8",
		},
		{
			ProtoName:         "Int16",
			InsaneConvertFunc: "AsInt",
			GoName:            "int16",
		},
		{
			ProtoName:         "Int32",
			InsaneConvertFunc: "AsInt",
			GoName:            "int32",
		},
		{
			ProtoName:         "Int64",
			InsaneConvertFunc: "AsInt",
			GoName:            "int64",
		},
		{
			ProtoName:         "Int128",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.Int128",
			IsComplexNumber:   true,
		},
		{
			ProtoName:         "Int256",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.Int256",
			IsComplexNumber:   true,
		},
		{
			ProtoName:         "UInt8",
			InsaneConvertFunc: "AsInt",
			GoName:            "uint8",
		},
		{
			ProtoName:         "UInt16",
			InsaneConvertFunc: "AsInt",
			GoName:            "uint16",
		},
		{
			ProtoName:         "UInt32",
			InsaneConvertFunc: "AsInt",
			GoName:            "uint32",
		},
		{
			ProtoName:         "UInt64",
			InsaneConvertFunc: "AsInt",
			GoName:            "uint64",
		},
		{
			ProtoName:         "UInt128",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.UInt128",
			IsComplexNumber:   true,
		},
		{
			ProtoName:         "UInt256",
			InsaneConvertFunc: "AsInt",
			GoName:            "proto.UInt256",
			IsComplexNumber:   true,
		},
		{
			ProtoName:         "Float32",
			InsaneConvertFunc: "AsInt",
			GoName:            "float32",
		},
		{
			ProtoName:         "Float64",
			InsaneConvertFunc: "AsInt",
			GoName:            "float64",
		},
	}}

	f, err := os.Create(outputFileName)
	if err != nil {
		logger.Fatal(err)
	}
	defer f.Close()

	if err := columnTemplate.Execute(f, data); err != nil {
		logger.Fatal(err)
	}
	logger.Info("done")
}
