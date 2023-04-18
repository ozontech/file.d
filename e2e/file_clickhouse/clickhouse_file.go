package file_clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Config struct {
	ctx    context.Context
	cancel func()

	inputDir   string
	conn       *ch.Client
	samples    []Sample
	sampleTime time.Time
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.ctx, c.cancel = context.WithTimeout(context.Background(), time.Minute*2)

	conn, err := ch.Dial(c.ctx, ch.Options{
		Address: "127.0.0.1:9001",
	})
	require.NoError(t, err)
	c.conn = conn

	err = conn.Do(c.ctx, ch.Query{
		Body: `DROP TABLE IF EXISTS test_table_insert`})
	require.NoError(t, err)

	err = conn.Do(c.ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS test_table_insert
		(
		    c1 String,
		    c2 Int8,
		    c3 Int16,
		    c4 Nullable(Int16),
		    c5 Nullable(String),
		    level Enum8('error'=1, 'warn'=2, 'info'=3, 'debug'=4),
		    ipv4 Nullable(IPv4),
		    ipv6 Nullable(IPv6),
		    ts DateTime,
		    ts_with_tz DateTime('Europe/Moscow'),
		    ts64 DateTime64(3),
		    ts64_auto DateTime64(9, 'UTC'),
			created_at DateTime64(6, 'UTC') DEFAULT now()
		) ENGINE = Memory`,
	})
	require.NoError(t, err)

	c.inputDir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.inputDir)
	input.Set("filename_pattern", "input.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	c.sampleTime = time.Now()
	c.samples = []Sample{
		{
			C1:       json.RawMessage(`"1"`),
			C2:       2,
			C3:       3,
			C4:       proto.NewNullable(int16(4)),
			C5:       proto.Null[string](),
			Level:    3,
			IPv4:     proto.NewNullable(proto.ToIPv4(netip.MustParseAddr("127.0.0.1"))),
			IPv6:     proto.NewNullable(proto.ToIPv6(netip.MustParseAddr("0000:0000:0000:0000:0000:0000:0000:0001"))),
			TS:       c.sampleTime,
			TSWithTZ: c.sampleTime,
			TS64:     c.sampleTime,
		},
		{
			C1:       json.RawMessage(`549023`),
			C2:       42,
			C3:       101,
			C4:       proto.NewNullable(int16(6)),
			C5:       proto.NewNullable("ping pong"),
			Level:    2,
			IPv4:     proto.Null[proto.IPv4](),
			IPv6:     proto.Null[proto.IPv6](),
			TS:       c.sampleTime,
			TSWithTZ: c.sampleTime,
			TS64:     c.sampleTime,
		},
		{
			C1:       json.RawMessage(`{"type":"append object as string"}`),
			C2:       42,
			C3:       101,
			C4:       proto.NewNullable(int16(5425)),
			C5:       proto.NewNullable("ok google"),
			Level:    1,
			IPv4:     proto.Null[proto.IPv4](),
			IPv6:     proto.Null[proto.IPv6](),
			TS:       c.sampleTime,
			TSWithTZ: c.sampleTime,
			TS64:     c.sampleTime,
		},
	}
}

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer func() {
		_ = file.Close()
	}()

	for i := range c.samples {
		sampleRaw, err := json.Marshal(&c.samples[i])
		require.NoError(t, err)
		_, err = file.Write(sampleRaw)
		require.NoError(t, err)
		_, err = file.WriteString("\n")
		require.NoError(t, err)
	}
	_ = file.Sync()
}

func (c *Config) Validate(t *testing.T) {
	var rows int
	for i := 0; i < 1000; i++ {
		cnt := proto.ColUInt64{}
		err := c.conn.Do(c.ctx, ch.Query{
			Body: `select count(*) from test_table_insert`,
			Result: proto.Results{
				{Name: "count()", Data: &cnt},
			},
		})
		require.NoError(t, err)

		rows = int(cnt.Row(0))
		if rows != len(c.samples) {
			time.Sleep(time.Millisecond * 10)
			continue
		}
	}

	assert.Equal(t, len(c.samples), rows)

	var (
		c1       = new(proto.ColStr)
		c2       = new(proto.ColInt8)
		c3       = new(proto.ColInt16)
		c4       = new(proto.ColInt16).Nullable()
		c5       = new(proto.ColStr).Nullable()
		level    = new(proto.ColEnum8)
		ipv4     = new(proto.ColIPv4).Nullable()
		ipv6     = new(proto.ColIPv6).Nullable()
		ts       = new(proto.ColDateTime)
		tsWithTz = new(proto.ColDateTime)
		ts64     = new(proto.ColDateTime64)
		ts64Auto = new(proto.ColDateTime64)
	)
	ts64.WithPrecision(proto.PrecisionMilli)
	ts64Auto.WithPrecision(proto.PrecisionNano)

	require.NoError(t, c.conn.Do(c.ctx, ch.Query{
		Body: `select c1, c2, c3, c4, c5, level, ipv4, ipv6, ts, ts_with_tz, ts64, ts64_auto
			from test_table_insert
			order by c1`,
		Result: proto.Results{
			proto.ResultColumn{Name: "c1", Data: c1},
			proto.ResultColumn{Name: "c2", Data: c2},
			proto.ResultColumn{Name: "c3", Data: c3},
			proto.ResultColumn{Name: "c4", Data: c4},
			proto.ResultColumn{Name: "c5", Data: c5},
			proto.ResultColumn{Name: "level", Data: level},
			proto.ResultColumn{Name: "ipv4", Data: ipv4},
			proto.ResultColumn{Name: "ipv6", Data: ipv6},
			proto.ResultColumn{Name: "ts", Data: ts},
			proto.ResultColumn{Name: "ts_with_tz", Data: tsWithTz},
			proto.ResultColumn{Name: "ts64", Data: ts64},
			proto.ResultColumn{Name: "ts64_auto", Data: ts64Auto},
		},
		OnResult: func(_ context.Context, _ proto.Block) error {
			return nil
		},
	}))

	assert.Equal(t, len(c.samples), c1.Rows())
	for i := 0; i < c1.Rows(); i++ {
		sample := c.samples[i]

		c1Expected, _ := json.Marshal(sample.C1)
		assert.Equal(t, string(trim(c1Expected)), c1.Row(i))

		assert.Equal(t, sample.C2, c2.Row(i))
		assert.Equal(t, sample.C3, c3.Row(i))
		assert.Equal(t, sample.C4, c4.Row(i))
		assert.Equal(t, sample.C5, c5.Row(i))
		assert.Equal(t, sample.IPv4, ipv4.Row(i))
		assert.Equal(t, sample.IPv6, ipv6.Row(i))
		assert.Equal(t, sample.TS.Unix(), ts.Row(i).Unix())

		assert.Equal(t, sample.TSWithTZ.Unix(), tsWithTz.Row(i).Unix())
		assert.Equal(t, "Europe/Moscow", tsWithTz.Row(i).Location().String())

		assert.Equal(t, sample.TS64.UnixMilli()*1e6, ts64.Row(i).UnixNano())
		assert.Equal(t, "Local", ts64.Row(i).Location().String())

		assert.True(t, ts64Auto.Row(i).After(sample.TS64), "%s before %s", ts64Auto.Row(i).String(), sample.TS64.String()) // we are use set_time plugin and override this value
		assert.Equal(t, "UTC", ts64Auto.Row(i).Location().String())
	}
}

func trim(val []byte) []byte {
	val = bytes.TrimPrefix(val, []byte("\""))
	val = bytes.TrimSuffix(val, []byte("\""))
	return val
}
