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

const (
	IPv4Name    = "proto.IPv4"
	IPv6Name    = "proto.IPv6"
	UUIDName    = "uuid.UUID"
	TimeName    = "time.Time"
	Int128Name  = "proto.Int128"
	UInt128Name = "proto.UInt128"
	Int256Name  = "proto.Int256"
	UInt256Name = "proto.UInt256"
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
		_, _ = fmt.Fprintf(os.Stderr, buf.String())
		logger.Panic(err)
	}

	f, err := os.Create(outputFileName)
	if err != nil {
		logger.Panic(err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	_, err = f.Write(result)
	if err != nil {
		logger.Panic(err)
	}

	logger.Info("done")
}

func clickhouseTypes() []Type {
	types := []Type{
		{
			ChTypeName: "Bool",
			GoName:     "bool",
			Nullable:   true,
		},
		{
			ChTypeName:     "String",
			GoName:         "string",
			Nullable:       true,
			CustomImpl:     true,
			LowCardinality: true,
		},
		{
			ChTypeName: "Enum8",
			GoName:     goTypeEnum,
			CustomImpl: true,
		},
		{
			ChTypeName: "Enum16",
			GoName:     goTypeEnum,
			CustomImpl: true,
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
				ChTypeName:  protoName,
				GoName:      goName,
				Convertable: true,
				Nullable:    true,
			})
		}
	}

	types = append(types,
		Type{
			ChTypeName:      "Int128",
			GoName:          Int128Name,
			Convertable:     true,
			Nullable:        true,
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "UInt128",
			GoName:          UInt128Name,
			Convertable:     true,
			Nullable:        true,
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "Int256",
			GoName:          Int256Name,
			Convertable:     true,
			Nullable:        true,
			isComplexNumber: true,
		},
		Type{
			ChTypeName:      "UInt256",
			GoName:          UInt256Name,
			Convertable:     true,
			Nullable:        true,
			isComplexNumber: true,
		},
		Type{
			ChTypeName: "Float32",
			GoName:     "float32",
			Nullable:   true,
		},
		Type{
			ChTypeName: "Float64",
			GoName:     "float64",
			Nullable:   true,
		},
		Type{
			ChTypeName: "DateTime",
			GoName:     TimeName,
			CustomImpl: true,
		},
		Type{
			ChTypeName: "DateTime64",
			GoName:     TimeName,
			CustomImpl: true,
		},
		Type{
			ChTypeName: "IPv4",
			GoName:     IPv4Name,
			Nullable:   true,
		},
		Type{
			ChTypeName: "IPv6",
			GoName:     IPv6Name,
			Nullable:   true,
		},
		Type{
			ChTypeName: "UUID",
			GoName:     UUIDName,
			Nullable:   true,
		},
	)

	return types
}
