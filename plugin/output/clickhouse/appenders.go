package clickhouse

import (
	"github.com/ClickHouse/ch-go/proto"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type StringAppender struct {
	*proto.ColStr
}

func (a StringAppender) Append(node *insaneJSON.StrictNode) error {
	val, err := node.AsString()
	if err != nil {
		return err
	}
	a.ColStr.Append(val)

	return nil
}

func (a StringAppender) Reset() {
	a.ColStr.Reset()
}

type Int8Appender struct {
	*proto.ColInt8
}

func (a Int8Appender) Append(node *insaneJSON.StrictNode) error {
	val, err := node.AsInt64()
	if err != nil {
		return err
	}
	a.ColInt8.Append(int8(val))
	return nil
}

func (a Int8Appender) Reset() {
	a.ColInt8.Reset()
}

func (a Int8Appender) Input() proto.ColInput {
	return a.ColInt8
}

type Int16Appender struct {
	*proto.ColInt16
}

func (a Int16Appender) Append(node *insaneJSON.StrictNode) error {
	val, err := node.AsInt64()
	if err != nil {
		return err
	}
	a.ColInt16.Append(int16(val))
	return nil
}

func (a Int16Appender) Reset() {
	a.ColInt16.Reset()
}

func (a Int16Appender) Input() proto.ColInput {
	return a.ColInt16
}

type EnumAppender struct {
	*proto.ColEnum
}

func (a EnumAppender) Append(node *insaneJSON.StrictNode) error {
	val, err := node.AsString()
	if err != nil {
		return err
	}
	a.ColEnum.Append(val)
	return nil
}

func (a EnumAppender) Reset() {
	a.ColEnum.Reset()
}

func (a EnumAppender) Input() proto.ColInput {
	return a.ColEnum
}
