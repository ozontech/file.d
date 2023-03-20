package main

import (
	"fmt"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

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
	b := new(Builder)
	// function signature
	// func NewColIPv4(
	b.WriteString(fmt.Sprintf("func New%s(", t.ColumnTypeName()))

	// ctor args: "nullable bool, someArg string"
	for i, arg := range t.CtorArgs() {
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(arg.Name)
		b.WriteString(" ")
		b.WriteString(arg.Type)
	}

	// ) *InsaneColIPv4 {
	b.WriteLine(fmt.Sprintf(") *%s {", t.ColumnTypeName()))

	// function body
	b.WriteLine(fmt.Sprintf("return &%s{", t.ColumnTypeName()))

	// initialize struct fields

	{
		mappingName := t.LibChTypeNameFull()
		if t.GoName == goTypeEnum {
			// from existing value
			mappingName = "col"
		} else {
			// new column
			mappingName = fmt.Sprintf("&%s{}", mappingName)
		}
		b.WriteLine(fmt.Sprintf("col: %s,", mappingName))
	}

	if !t.CannotBeNull {
		b.WriteLine(fmt.Sprintf("nullCol: proto.NewColNullable(proto.ColumnOf[%s](&%s{})),", t.GoName, t.LibChTypeNameFull()))
		b.WriteLine("nullable: nullable,")
	}

	b.WriteLine("}")
	b.WriteLine("}")

	return b.String()
}

func (t Type) CallCtor() string {
	var args strings.Builder
	for _, arg := range t.CtorArgs() {
		args.WriteString(arg.Name)
	}
	return fmt.Sprintf("New%s(%s)", t.ColumnTypeName(), args.String())
}

func (t Type) ConvertInsaneJSONValue() string {
	// input variable name to convert is "v"
	// output variable name must be "val"
	if t.isComplexNumber {
		return fmt.Sprintf("val := %sFromInt(v)", t.GoName)
	}
	if t.GoName == goTypeTime {
		return "val := time.Unix(int64(v), 0)"
	}
	if t.GoName == goTypeIPv4 || t.GoName == goTypeIPv6 {
		ipVer := "4"
		if t.GoName == goTypeIPv6 {
			ipVer = "6"
		}

		return fmt.Sprintf(`addr, err := netip.ParseAddr(v)
if err != nil {
	return err
}
if !addr.Is%s() {
	return fmt.Errorf("invalid IPv%s value, val=%%s", v)
}
val := proto.ToIPv%s(addr)`, ipVer, ipVer, ipVer)
	}
	if t.GoName == goTypeEnum {
		return "val := v"
	}
	return fmt.Sprintf("val := %s(v)", t.GoName)
}

func (t Type) InsaneConvertFunc() string {
	switch t.GoName {
	case "bool":
		return "AsBool"
	case "string", goTypeEnum, goTypeIPv4, goTypeIPv6:
		return "AsString"
	case goTypeTime:
		return "AsInt"
	default:
		return "AsInt"
	}
}

func (t Type) Preparable() bool {
	return t.GoName == goTypeEnum
}

type Builder struct {
	strings.Builder
}

func (b *Builder) WriteLine(str string) {
	b.WriteString(str)
	_ = b.WriteByte('\n')
}

func (b *Builder) WriteString(str string) {
	_, _ = b.Builder.WriteString(str)
}
