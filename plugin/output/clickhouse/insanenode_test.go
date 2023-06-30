package clickhouse

import (
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func TestZeroValueNode(t *testing.T) {
	r := require.New(t)
	zeroValue := ZeroValueNode{}
	buf := new(proto.Buffer)
	utcLoc, err := time.LoadLocation("UTC")
	r.NoError(err)
	enum8 := new(proto.ColEnum)
	r.NoError(enum8.Infer("Enum8('error'=1, 'info'=2, ''=3)"))
	enum16 := new(proto.ColEnum)
	r.NoError(enum16.Infer("Enum16('error'=1, 'info'=2, ''=3)"))

	columns := []InsaneColInput{
		NewColDateTime(&proto.ColDateTime{Location: utcLoc}),
		NewColDateTime64(
			(&proto.ColDateTime64{Location: utcLoc}).WithPrecision(proto.PrecisionSecond),
			proto.PrecisionSecond.Scale(),
		),
		NewColEnum8(enum8),
		NewColEnum16(enum16),
		NewColStringArray(),
	}

	for _, nullable := range []bool{true, false} {
		columns = append(columns, []InsaneColInput{
			NewColUUID(nullable),
			NewColString(nullable, true),
			NewColFloat32(nullable),
			NewColFloat64(nullable),
			NewColInt8(nullable),
			NewColUInt8(nullable),
			NewColInt16(nullable),
			NewColUInt16(nullable),
			NewColInt32(nullable),
			NewColUInt32(nullable),
			NewColInt128(nullable),
			NewColUInt128(nullable),
			NewColIPv4(nullable),
			NewColIPv6(nullable),
		}...)
	}

	for _, col := range columns {
		buf.Reset()
		r.NoError(col.Append(zeroValue))
		r.Equal(col.Rows(), 1, "col=%T", col)

		if col, ok := col.(proto.Preparable); ok {
			r.NoError(col.Prepare())
		}

		col.EncodeColumn(buf)
		r.NotEqual(0, len(buf.Buf), "col=%T", col)
	}
}
