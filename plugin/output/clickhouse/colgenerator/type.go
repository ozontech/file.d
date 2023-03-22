package main

import (
	"fmt"
)

type Type struct {
	// ChTypeName is Clickhouse type name e.g. String, Int32
	ChTypeName string
	// Go name of the type, e.g. int8, string
	GoName string
	// CannotConvert can not cast to Go type
	CannotConvert bool
	// CannotBeNull
	CannotBeNull bool
	// isComplexNumber integers with 128-256 bits
	isComplexNumber bool
	// CustomImpl skips ctor and struct generation if truth
	CustomImpl bool
}

func (t Type) ColumnTypeName() string {
	return "Col" + t.ChTypeName
}

func (t Type) ConvertInsaneJSONValue() string {
	if t.isComplexNumber {
		return fmt.Sprintf("%sFromInt", t.GoName)
	}
	return t.GoName
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

// Preparable returns truth if the column must contain Prepare function
func (t Type) Preparable() bool {
	return t.GoName == goTypeEnum
}

func (t Type) libChTypeName() string {
	if t.ChTypeName == "String" {
		// 'String' named as 'Str' in the ch-go library
		return "Str"
	}
	if t.GoName == goTypeEnum {
		return "Enum"
	}
	return t.ChTypeName
}

func (t Type) LibChTypeNameFull() string {
	return "proto.Col" + t.libChTypeName()
}

func (t Type) NullableTypeName() string {
	return fmt.Sprintf("proto.ColNullable[%s]", t.GoName)
}
