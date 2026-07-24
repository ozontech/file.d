package clickhouse

import (
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	insaneJSON "github.com/ozontech/insane-json"
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

	columns := make([]InsaneColInput, 0, 38)
	columns = append(columns, []InsaneColInput{
		NewColDateTime(&proto.ColDateTime{Location: utcLoc}),
		NewColDateTime64(
			(&proto.ColDateTime64{Location: utcLoc}).WithPrecision(proto.PrecisionSecond),
			proto.PrecisionSecond,
		),
		NewColEnum8(enum8),
		NewColEnum16(enum16),
		NewColStringArray(),
		NewColMapStringString(),
	}...)

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
			NewColInt64(nullable),
			NewColUInt64(nullable),
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

func TestNodeAsTime(t *testing.T) {
	r := require.New(t)
	test := func(jsonTime string, precision proto.Precision, expected time.Time) {
		t.Helper()

		root, err := insaneJSON.DecodeString(jsonTime)
		r.NoError(err)
		defer insaneJSON.Release(root)

		n := StrictNode{root.MutateToStrict()}

		ts, err := n.AsTime(precision)
		r.NoError(err)
		r.Equal(expected.Local(), ts.Local())
	}

	test(`"2004-09-16T23:59:58.75Z"`, 10, mustParseTime("2004-09-16T23:59:58.75Z"))
	test(`1740643609`, proto.PrecisionSecond, mustParseTime("2025-02-27T08:06:49.00Z"))
	test(`"1740643609"`, proto.PrecisionSecond, mustParseTime("2025-02-27T08:06:49.00Z"))
}

func TestNodeAsKVArray(t *testing.T) {
	r := require.New(t)

	root, err := insaneJSON.DecodeString(`{"b":"1","c":"2","b":"3","a":"4"}`)
	defer insaneJSON.Release(root)
	r.NoError(err)

	// expected sorted keys
	want := []proto.KV[string, string]{
		{Key: "a", Value: "4"},
		{Key: "b", Value: "1"},
		{Key: "b", Value: "3"},
		{Key: "c", Value: "2"},
	}

	n := NonStrictNode{root.Node}
	got1, err := n.AsKVArray()
	r.NoError(err)
	r.Equal(want, got1)

	sn := StrictNode{root.MutateToStrict()}
	got2, err := sn.AsKVArray()
	r.NoError(err)
	r.Equal(want, got2)
}

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.Local()
}
