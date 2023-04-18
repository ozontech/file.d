package clickhouse

import (
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	insaneJSON "github.com/vitkovskii/insane-json"
)

var (
	ErrNodeIsNil        = errors.New("node is nil, but column is not")
	ErrInvalidIPVersion = errors.New("IP is valid, but the version does not match the column")
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

func (t *ColDateTime) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	v, err := node.AsInt()
	if err != nil {
		return err
	}

	val := time.Unix(int64(v), 0)

	t.col.Append(val)

	return nil
}

// ColDateTime64 represents Clickhouse DateTime64 type.
type ColDateTime64 struct {
	col   *proto.ColDateTime64
	scale int64
}

func NewColDateTime64(col *proto.ColDateTime64, scale int64) *ColDateTime64 {
	return &ColDateTime64{
		col:   col,
		scale: scale,
	}
}

func (t *ColDateTime64) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	v, err := node.AsInt64()
	if err != nil {
		return err
	}

	nsec := v * t.scale
	val := time.Unix(nsec/1e9, nsec%1e9)

	t.col.Append(val)

	return nil
}

// ColIPv4 represents Clickhouse IPv4 type.
type ColIPv4 struct {
	col      *proto.ColIPv4
	nullCol  *proto.ColNullable[proto.IPv4]
	nullable bool
}

func NewColIPv4(nullable bool) *ColIPv4 {
	return &ColIPv4{
		col:      new(proto.ColIPv4),
		nullCol:  new(proto.ColIPv4).Nullable(),
		nullable: nullable,
	}
}

func (t *ColIPv4) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.IPv4]())
		return nil
	}

	addr, err := ipFromNode(node)
	if err != nil {
		return err
	}

	if !addr.Is4() {
		return ErrInvalidIPVersion
	}

	val := proto.ToIPv4(addr)

	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

// ColIPv6 represents Clickhouse IPv6 type.
type ColIPv6 struct {
	col      *proto.ColIPv6
	nullCol  *proto.ColNullable[proto.IPv6]
	nullable bool
}

func NewColIPv6(nullable bool) *ColIPv6 {
	return &ColIPv6{
		col:      new(proto.ColIPv6),
		nullCol:  new(proto.ColIPv6).Nullable(),
		nullable: nullable,
	}
}

func (t *ColIPv6) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.IPv6]())
		return nil
	}

	addr, err := ipFromNode(node)
	if err != nil {
		return err
	}

	if !addr.Is6() {
		return ErrInvalidIPVersion
	}

	val := proto.ToIPv6(addr)

	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
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

func (t *ColEnum8) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}
	val, err := node.AsString()
	if err != nil {
		return err
	}
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

func (t *ColEnum16) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}
	val, err := node.AsString()
	if err != nil {
		return err
	}
	t.col.Append(val)

	return nil
}

// ColString represents Clickhouse String type.
type ColString struct {
	col      *proto.ColStr
	nullCol  *proto.ColNullable[string]
	nullable bool
	strict   bool
}

func NewColString(nullable bool, strict bool) *ColString {
	return &ColString{
		col:      new(proto.ColStr),
		nullCol:  new(proto.ColStr).Nullable(),
		nullable: nullable,
		strict:   strict,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColString) Append(node *insaneJSON.StrictNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[string]())
		return nil
	}

	val, err := node.AsString()
	// if it is not a string
	if err != nil {
		if t.strict {
			return fmt.Errorf("disable strict mode to append any value to the String column")
		}
		val = node.EncodeToString()
	}

	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
	} else {
		t.col.Append(val)
	}

	return nil
}

func ipFromNode(node *insaneJSON.StrictNode) (netip.Addr, error) {
	v, err := node.AsString()
	if err != nil {
		return netip.Addr{}, err
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("extract ip form json node: %w", err)
	}

	return addr, nil
}
