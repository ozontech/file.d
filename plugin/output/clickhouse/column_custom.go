package clickhouse

import (
	"errors"
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
)

var (
	ErrNodeIsNil = errors.New("node is nil, but column is not")
)

// ColDateTime represents Clickhouse DateTime type.
type ColDateTime struct {
	col *proto.ColDateTime
}

func NewColDateTime(col *proto.ColDateTime) *ColDateTime {
	return &ColDateTime{
		col: col,
	}
}

func (t *ColDateTime) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	val, err := node.AsTime(proto.PrecisionSecond)
	if err != nil {
		return fmt.Errorf("converting node to time: %w", err)
	}

	t.col.Append(val)

	return nil
}

// ColDateTime64 represents Clickhouse DateTime64 type.
type ColDateTime64 struct {
	col  *proto.ColDateTime64
	prec proto.Precision
}

func NewColDateTime64(col *proto.ColDateTime64, prec proto.Precision) *ColDateTime64 {
	return &ColDateTime64{
		col:  col,
		prec: prec,
	}
}

func (t *ColDateTime64) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	val, err := node.AsTime(t.prec)
	if err != nil {
		return fmt.Errorf("converting to time: %w", err)
	}

	t.col.Append(val)

	return nil
}

// ColEnum8 represents Clickhouse Enum8 type.
type ColEnum8 struct {
	col *proto.ColEnum
}

func NewColEnum8(col *proto.ColEnum) *ColEnum8 {
	return &ColEnum8{
		col: col,
	}
}

func (t *ColEnum8) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}
	val, err := node.AsString()
	if err != nil {
		return err
	}

	// TODO: check that this val is valid for the enum
	t.col.Append(val)

	return nil
}

// ColEnum16 represents Clickhouse Enum16 type.
type ColEnum16 struct {
	col *proto.ColEnum
}

func NewColEnum16(col *proto.ColEnum) *ColEnum8 {
	return &ColEnum8{
		col: col,
	}
}

func (t *ColEnum16) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}
	val, err := node.AsString()
	if err != nil {
		return err
	}

	// TODO: check that this val is valid for the enum
	t.col.Append(val)

	return nil
}

// ColString represents Clickhouse String type.
type ColString struct {
	// col contains values for the String type.
	col *proto.ColStr

	// nullCol contains nullable values for the Nullable(String) type.
	nullCol *proto.ColNullable[string]
	// nullable the truth if the column is nullable.
	nullable bool

	// lcCol contains LowCardinality values for the LowCardinality(proto.ColLowCardinality[String]) type.
	lcCol *proto.ColLowCardinality[string]
	// lc the truth if the column is LowCardinality.
	lc bool
}

var _ proto.StateEncoder = (*ColString)(nil)

func NewColString(nullable, lowCardinality bool) *ColString {
	return &ColString{
		col:      new(proto.ColStr),
		nullCol:  new(proto.ColStr).Nullable(),
		nullable: nullable,
		lcCol:    new(proto.ColStr).LowCardinality(),
		lc:       lowCardinality,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColString) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[string]())
		return nil
	}

	val, err := node.AsString()
	if err != nil {
		return fmt.Errorf("converting node to the string: %w", err)
	}

	switch {
	case t.nullable:
		t.nullCol.Append(proto.NewNullable(val))
	case t.lc:
		t.lcCol.Append(val)
	default:
		t.col.Append(val)
	}

	return nil
}

func (t *ColString) EncodeState(b *proto.Buffer) {
	if t.lc {
		t.lcCol.EncodeState(b)
	}
}
