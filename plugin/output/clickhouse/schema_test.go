package clickhouse

import (
	"context"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/stretchr/testify/require"
)

func Test_parseSchema(t *testing.T) {
	schema := Schema{Columns: []Column{
		{
			Name: "message",
			Type: "String",
		},
		{ // { schema: [ {"name": "level", "type": "LowCardinality(String)"} ] ] }
			Name: "level",
			Type: "LowCardinality(String)",
		},
		{
			Name: "time",
			Type: "DateTime",
		},
	}}
	expected := proto.Input{
		proto.InputColumn{
			Name: "message",
			Data: new(proto.ColStr),
		},
		proto.InputColumn{
			Name: "level",
			Data: proto.NewLowCardinality[string](new(proto.ColStr)),
		},
		proto.InputColumn{
			Name: "time",
			Data: new(proto.ColDateTime),
		},
	}

	got, err := parseSchema(schema)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestClickouse(t *testing.T) {
	opts, err := clickhouse.ParseDSN("clickhouse://localhost:9000")
	require.NoError(t, err)
	driver, err := clickhouse.Open(opts)
	require.NoError(t, err)
	err = driver.Exec(context.Background(), "insert into test3 (A,B,C,D) values($1, $2, $3, $4)", 1, 2, 3, 4)
	require.NoError(t, err)
}
