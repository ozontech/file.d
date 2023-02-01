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
	Count    int

	conn *ch.Client
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	ctx := context.Background()

	conn, err := ch.Dial(ctx, ch.Options{})
	require.NoError(t, err)
	c.conn = conn

	err = conn.Do(ctx, ch.Query{
		Body: `DROP TABLE IF EXISTS test_table_insert`})

	err = conn.Do(ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS test_table_insert
(
    c1 String,
    c2 Int8,
    c3 Int16
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
	[]byte(`{ "c1": "1", "c2": 2, "c3": 3 }`),
	[]byte(`{ "c1": "hello world", "c2": 42, "c3": 24 }`),
}

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer file.Close()

	for i := 0; i < c.Count; i++ {
		_, err = file.Write(samples[i%len(samples)])
		require.NoError(t, err)
		_, err = file.WriteString("\n")
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	time.Sleep(time.Second * 5)
	ctx := context.Background()

	cnt := proto.ColUInt64{}
	err := c.conn.Do(ctx, ch.Query{
		Body: `select count(*) from test_table_insert;`,
		Result: proto.Results{
			{Name: "count()", Data: &cnt},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(c.Count), cnt.Row(0))
}
