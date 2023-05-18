// Code generated by "./colgenerator"; DO NOT EDIT.

package clickhouse

import (
	"github.com/ClickHouse/ch-go/proto"
	insaneJSON "github.com/vitkovskii/insane-json"
)

// ColBool represents Clickhouse Bool type.
type ColBool struct {
	col      *proto.ColBool
	nullCol  *proto.ColNullable[bool]
	nullable bool
}

var _ InsaneColInput = (*ColBool)(nil)

func NewColBool(nullable bool) *ColBool {
	return &ColBool{
		col:      new(proto.ColBool),
		nullCol:  new(proto.ColBool).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColBool) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[bool]())
		return nil
	}
	val := node.AsBool()
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColBool) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColBool) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColBool) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColBool) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColString defined and implemented in another file

var _ InsaneColInput = (*ColString)(nil)

func (t *ColString) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColString) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColString) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColString) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColEnum8 defined and implemented in another file

var _ InsaneColInput = (*ColEnum8)(nil)
var _ proto.Preparable = (*ColEnum8)(nil)

func (t *ColEnum8) Reset() {
	t.col.Reset()
}

func (t *ColEnum8) Type() proto.ColumnType {
	return t.col.Type()
}

func (t *ColEnum8) Rows() int {
	return t.col.Rows()
}

func (t *ColEnum8) EncodeColumn(buffer *proto.Buffer) {
	t.col.EncodeColumn(buffer)
}

// Prepare the column before sending.
func (t *ColEnum8) Prepare() error {
	return t.col.Prepare()
}

// struct ColEnum16 defined and implemented in another file

var _ InsaneColInput = (*ColEnum16)(nil)
var _ proto.Preparable = (*ColEnum16)(nil)

func (t *ColEnum16) Reset() {
	t.col.Reset()
}

func (t *ColEnum16) Type() proto.ColumnType {
	return t.col.Type()
}

func (t *ColEnum16) Rows() int {
	return t.col.Rows()
}

func (t *ColEnum16) EncodeColumn(buffer *proto.Buffer) {
	t.col.EncodeColumn(buffer)
}

// Prepare the column before sending.
func (t *ColEnum16) Prepare() error {
	return t.col.Prepare()
}

// ColInt8 represents Clickhouse Int8 type.
type ColInt8 struct {
	col      *proto.ColInt8
	nullCol  *proto.ColNullable[int8]
	nullable bool
}

var _ InsaneColInput = (*ColInt8)(nil)

func NewColInt8(nullable bool) *ColInt8 {
	return &ColInt8{
		col:      new(proto.ColInt8),
		nullCol:  new(proto.ColInt8).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt8) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[int8]())
		return nil
	}
	val := int8(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt8) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt8) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt8) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt8) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt8 represents Clickhouse UInt8 type.
type ColUInt8 struct {
	col      *proto.ColUInt8
	nullCol  *proto.ColNullable[uint8]
	nullable bool
}

var _ InsaneColInput = (*ColUInt8)(nil)

func NewColUInt8(nullable bool) *ColUInt8 {
	return &ColUInt8{
		col:      new(proto.ColUInt8),
		nullCol:  new(proto.ColUInt8).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt8) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[uint8]())
		return nil
	}
	val := uint8(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt8) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt8) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt8) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt8) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColInt16 represents Clickhouse Int16 type.
type ColInt16 struct {
	col      *proto.ColInt16
	nullCol  *proto.ColNullable[int16]
	nullable bool
}

var _ InsaneColInput = (*ColInt16)(nil)

func NewColInt16(nullable bool) *ColInt16 {
	return &ColInt16{
		col:      new(proto.ColInt16),
		nullCol:  new(proto.ColInt16).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt16) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[int16]())
		return nil
	}
	val := int16(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt16) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt16) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt16) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt16) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt16 represents Clickhouse UInt16 type.
type ColUInt16 struct {
	col      *proto.ColUInt16
	nullCol  *proto.ColNullable[uint16]
	nullable bool
}

var _ InsaneColInput = (*ColUInt16)(nil)

func NewColUInt16(nullable bool) *ColUInt16 {
	return &ColUInt16{
		col:      new(proto.ColUInt16),
		nullCol:  new(proto.ColUInt16).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt16) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[uint16]())
		return nil
	}
	val := uint16(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt16) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt16) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt16) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt16) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColInt32 represents Clickhouse Int32 type.
type ColInt32 struct {
	col      *proto.ColInt32
	nullCol  *proto.ColNullable[int32]
	nullable bool
}

var _ InsaneColInput = (*ColInt32)(nil)

func NewColInt32(nullable bool) *ColInt32 {
	return &ColInt32{
		col:      new(proto.ColInt32),
		nullCol:  new(proto.ColInt32).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt32) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[int32]())
		return nil
	}
	val := int32(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt32) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt32) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt32) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt32) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt32 represents Clickhouse UInt32 type.
type ColUInt32 struct {
	col      *proto.ColUInt32
	nullCol  *proto.ColNullable[uint32]
	nullable bool
}

var _ InsaneColInput = (*ColUInt32)(nil)

func NewColUInt32(nullable bool) *ColUInt32 {
	return &ColUInt32{
		col:      new(proto.ColUInt32),
		nullCol:  new(proto.ColUInt32).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt32) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[uint32]())
		return nil
	}
	val := uint32(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt32) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt32) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt32) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt32) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColInt64 represents Clickhouse Int64 type.
type ColInt64 struct {
	col      *proto.ColInt64
	nullCol  *proto.ColNullable[int64]
	nullable bool
}

var _ InsaneColInput = (*ColInt64)(nil)

func NewColInt64(nullable bool) *ColInt64 {
	return &ColInt64{
		col:      new(proto.ColInt64),
		nullCol:  new(proto.ColInt64).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt64) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[int64]())
		return nil
	}
	val := int64(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt64) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt64) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt64) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt64) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt64 represents Clickhouse UInt64 type.
type ColUInt64 struct {
	col      *proto.ColUInt64
	nullCol  *proto.ColNullable[uint64]
	nullable bool
}

var _ InsaneColInput = (*ColUInt64)(nil)

func NewColUInt64(nullable bool) *ColUInt64 {
	return &ColUInt64{
		col:      new(proto.ColUInt64),
		nullCol:  new(proto.ColUInt64).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt64) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[uint64]())
		return nil
	}
	val := uint64(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt64) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt64) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt64) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt64) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColInt128 represents Clickhouse Int128 type.
type ColInt128 struct {
	col      *proto.ColInt128
	nullCol  *proto.ColNullable[proto.Int128]
	nullable bool
}

var _ InsaneColInput = (*ColInt128)(nil)

func NewColInt128(nullable bool) *ColInt128 {
	return &ColInt128{
		col:      new(proto.ColInt128),
		nullCol:  new(proto.ColInt128).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt128) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.Int128]())
		return nil
	}
	val := proto.Int128FromInt(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt128) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt128) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt128) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt128) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt128 represents Clickhouse UInt128 type.
type ColUInt128 struct {
	col      *proto.ColUInt128
	nullCol  *proto.ColNullable[proto.UInt128]
	nullable bool
}

var _ InsaneColInput = (*ColUInt128)(nil)

func NewColUInt128(nullable bool) *ColUInt128 {
	return &ColUInt128{
		col:      new(proto.ColUInt128),
		nullCol:  new(proto.ColUInt128).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt128) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.UInt128]())
		return nil
	}
	val := proto.UInt128FromInt(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt128) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt128) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt128) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt128) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColInt256 represents Clickhouse Int256 type.
type ColInt256 struct {
	col      *proto.ColInt256
	nullCol  *proto.ColNullable[proto.Int256]
	nullable bool
}

var _ InsaneColInput = (*ColInt256)(nil)

func NewColInt256(nullable bool) *ColInt256 {
	return &ColInt256{
		col:      new(proto.ColInt256),
		nullCol:  new(proto.ColInt256).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColInt256) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.Int256]())
		return nil
	}
	val := proto.Int256FromInt(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColInt256) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColInt256) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColInt256) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColInt256) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// ColUInt256 represents Clickhouse UInt256 type.
type ColUInt256 struct {
	col      *proto.ColUInt256
	nullCol  *proto.ColNullable[proto.UInt256]
	nullable bool
}

var _ InsaneColInput = (*ColUInt256)(nil)

func NewColUInt256(nullable bool) *ColUInt256 {
	return &ColUInt256{
		col:      new(proto.ColUInt256),
		nullCol:  new(proto.ColUInt256).Nullable(),
		nullable: nullable,
	}
}

// Append the insaneJSON.Node to the batch.
func (t *ColUInt256) Append(node *insaneJSON.Node) error {
	if node == nil || node.IsNull() {
		if !t.nullable {
			return ErrNodeIsNil
		}
		t.nullCol.Append(proto.Null[proto.UInt256]())
		return nil
	}
	val := proto.UInt256FromInt(node.AsInt())
	if t.nullable {
		t.nullCol.Append(proto.NewNullable(val))
		return nil
	}
	t.col.Append(val)

	return nil
}

func (t *ColUInt256) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColUInt256) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColUInt256) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColUInt256) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColFloat32 defined and implemented in another file

var _ InsaneColInput = (*ColFloat32)(nil)

func (t *ColFloat32) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColFloat32) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColFloat32) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColFloat32) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColFloat64 defined and implemented in another file

var _ InsaneColInput = (*ColFloat64)(nil)

func (t *ColFloat64) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColFloat64) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColFloat64) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColFloat64) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColDateTime defined and implemented in another file

var _ InsaneColInput = (*ColDateTime)(nil)

func (t *ColDateTime) Reset() {
	t.col.Reset()
}

func (t *ColDateTime) Type() proto.ColumnType {
	return t.col.Type()
}

func (t *ColDateTime) Rows() int {
	return t.col.Rows()
}

func (t *ColDateTime) EncodeColumn(buffer *proto.Buffer) {
	t.col.EncodeColumn(buffer)
}

// struct ColDateTime64 defined and implemented in another file

var _ InsaneColInput = (*ColDateTime64)(nil)

func (t *ColDateTime64) Reset() {
	t.col.Reset()
}

func (t *ColDateTime64) Type() proto.ColumnType {
	return t.col.Type()
}

func (t *ColDateTime64) Rows() int {
	return t.col.Rows()
}

func (t *ColDateTime64) EncodeColumn(buffer *proto.Buffer) {
	t.col.EncodeColumn(buffer)
}

// struct ColIPv4 defined and implemented in another file

var _ InsaneColInput = (*ColIPv4)(nil)

func (t *ColIPv4) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColIPv4) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColIPv4) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColIPv4) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}

// struct ColIPv6 defined and implemented in another file

var _ InsaneColInput = (*ColIPv6)(nil)

func (t *ColIPv6) Reset() {
	t.col.Reset()
	t.nullCol.Reset()
}

func (t *ColIPv6) Type() proto.ColumnType {
	if t.nullable {
		return t.nullCol.Type()
	}
	return t.col.Type()
}

func (t *ColIPv6) Rows() int {
	if t.nullable {
		return t.nullCol.Rows()
	}
	return t.col.Rows()
}

func (t *ColIPv6) EncodeColumn(buffer *proto.Buffer) {
	if t.nullable {
		t.nullCol.EncodeColumn(buffer)
		return
	}
	t.col.EncodeColumn(buffer)
}
