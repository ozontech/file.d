package clickhouse

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

func Test_parseSchema(t *testing.T) {
	schema := Schema{Columns: []Column{
		{
			Name: "message",
			Type: "String",
		},
		{
			// { schema: [ {"name": "level", "type": "LowCardinality(String)"} ] ] }
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

	got, err := inferInsaneColInputs(schema)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}
