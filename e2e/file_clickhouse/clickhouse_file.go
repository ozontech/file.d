package file_clickhouse

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

type Config struct {
	inputDir string
	count    int

	conn *ch.Client
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.count = 256
	ctx := context.Background()

	conn, err := ch.Dial(ctx, ch.Options{
		Address: "127.0.0.1:9001",
	})
	require.NoError(t, err)
	c.conn = conn

	err = conn.Do(ctx, ch.Query{
		Body: `DROP TABLE IF EXISTS test_table_insert`})
	require.NoError(t, err)

	err = conn.Do(ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS test_table_insert
(
    c1 String,
    c2 Int8,
    c3 Int16,
    c4 Nullable(Int16),
    c5 Nullable(String)
) ENGINE = Memory;`})
	require.NoError(t, err)

	c.inputDir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.inputDir)
	input.Set("filename_pattern", "input.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))
}

var samples = [][]byte{
	[]byte(`{ "c1": "str", "c2": 2, "c3": 3, "c4": 2, "c5": null }`),
	[]byte(`{ "c1": "str", "c2": 42, "c3": 24, "c4": null, "c5": null }`),
	[]byte(`{ "c1": "str", "c2": 8, "c3": 1, "c4": null, "c5": "nullable" }`),
}

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer file.Close()

	for i := 0; i < c.count; i++ {
		_, err = file.Write(samples[i%len(samples)])
		require.NoError(t, err)
		_, err = file.WriteString("\n")
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	ctx := context.Background()

	var rows int
	for i := 0; i < 100; i++ {
		cnt := proto.ColUInt64{}
		err := c.conn.Do(ctx, ch.Query{
			Body: `select count(*) from test_table_insert;`,
			Result: proto.Results{
				{Name: "count()", Data: &cnt},
			},
		})
		require.NoError(t, err)

		rows = int(cnt.Row(0))
		if rows != c.count {
			time.Sleep(time.Millisecond * 100)
			continue
		}
	}

	require.Equal(t, c.count, rows)
	time.Sleep(time.Second * 2)
}
