package clickhouse

import (
	"errors"
	"fmt"
	"net/netip"
	"strconv"
	"time"

	"github.com/ClickHouse/ch-go/proto"
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

func (t *ColDateTime) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	var val time.Time
	switch {
	case node.IsNumber():
		nodeVal, err := node.AsInt64()
		if err != nil {
			return err
		}

		val = time.Unix(nodeVal, 0)
	case node.IsString():
		var err error
		val, err = parseRFC3339Nano(node)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("value=%q is not a string or number", node.EncodeToString())
	}

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

func (t *ColDateTime64) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		return ErrNodeIsNil
	}

	var val time.Time
	switch {
	case node.IsNumber():
		v, err := node.AsInt64()
		if err != nil {
			return err
		}

		// convert to nanoseconds
		nsec := v * t.scale
		val = time.Unix(nsec/1e9, nsec%1e9)
	case node.IsString():
		var err error
		val, err = parseRFC3339Nano(node)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("value=%q is not a string or number", node.EncodeToString())
	}

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

func (t *ColIPv4) Append(node InsaneNode) error {
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

func (t *ColIPv6) Append(node InsaneNode) error {
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

func (t *ColEnum8) Append(node InsaneNode) error {
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

func (t *ColEnum16) Append(node InsaneNode) error {
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

	var val string
	if node.IsString() {
		var err error
		val, err = node.AsString()
		if err != nil {
			return err
		}
	} else {
		val = node.EncodeToString()
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

// ColFloat32 represents Clickhouse Float32 type.
type ColFloat32 struct {
	col      *proto.ColFloat32
	nullCol  *proto.ColNullable[float32]
	nullable bool
}

func NewColFloat32(nullable bool) *ColFloat32 {
	return &ColFloat32{
		col:      new(proto.ColFloat32),
		nullCol:  new(proto.ColFloat32).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColFloat32) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[float32]())
		return nil
	}
	v, err := node.AsString()
	if err != nil {
		return err
	}

	val, err := strconv.ParseFloat(v, 32)
	if err != nil {
		return fmt.Errorf("parse float (val=%q): %w", v, err)
	}

	if t.nullable {
		t.nullCol.Append(proto.NewNullable(float32(val)))
		return nil
	}
	t.col.Append(float32(val))

	return nil
}

// ColFloat64 represents Clickhouse Float64 type.
type ColFloat64 struct {
	col      *proto.ColFloat64
	nullCol  *proto.ColNullable[float64]
	nullable bool
}

func NewColFloat64(nullable bool) *ColFloat64 {
	return &ColFloat64{
		col:      new(proto.ColFloat64),
		nullCol:  new(proto.ColFloat64).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColFloat64) Append(node InsaneNode) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[float64]())
		return nil
	}
	v, err := node.AsString()
	if err != nil {
		return err
	}

	val, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fmt.Errorf("parse float (val=%q): %w", v, err)
	}

	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func ipFromNode(node InsaneNode) (netip.Addr, error) {
	v, err := node.AsString()
	if err != nil {
		return netip.Addr{}, err
	}

	addr, err := netip.ParseAddr(v)
	if err != nil {
		return netip.Addr{}, fmt.Errorf("extract ip form json node (val=%q): %w", node.EncodeToString(), err)
	}

	return addr, nil
}

func parseRFC3339Nano(node InsaneNode) (time.Time, error) {
	nodeVal, err := node.AsString()
	if err != nil {
		return time.Time{}, err
	}

	t, err := time.Parse(time.RFC3339Nano, nodeVal)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time as RFC3339Nano: %w", err)
	}
	return t, nil
}
