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
	"github.com/google/uuid"
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
	r := require.New(t)
	c.ctx, c.cancel = context.WithTimeout(context.Background(), time.Minute*2)

	conn, err := ch.Dial(c.ctx, ch.Options{
		Address: "127.0.0.1:9001",
	})
	r.NoError(err)
	c.conn = conn

	err = conn.Do(c.ctx, ch.Query{
		Body: `DROP TABLE IF EXISTS test_table_insert`})
	r.NoError(err)

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
		    ts_rfc3339nano DateTime64(9),
		    f32 Float32,
		    f64 Float64,
		    lc_str LowCardinality(String),
		    str_arr Array(String),
			map_str_str Map(String, String),
		    uuid UUID,
		    uuid_nullable Nullable(UUID),
			created_at DateTime64(6, 'UTC') DEFAULT now()
		) ENGINE = Memory`,
	})
	r.NoError(err)

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
			F32:      123.45,
			F64:      0.6789,
			LcStr:    "0558cee0-dd11-4304-9a15-1ad53d151fed",
			StrArr:   &[]string{"improve", "error handling"},
			MapStrStr: map[string]string{
				"key1": "val1",
				"key2": "val2",
			},
			UUID:         uuid.New(),
			UUIDNullable: uuid.NullUUID{UUID: uuid.New(), Valid: true},
		},
		{
			C1:           json.RawMessage(`549023`),
			C2:           42,
			C3:           101,
			C4:           proto.NewNullable(int16(6)),
			C5:           proto.NewNullable("ping pong"),
			Level:        2,
			IPv4:         proto.Null[proto.IPv4](),
			IPv6:         proto.Null[proto.IPv6](),
			TS:           c.sampleTime,
			TSWithTZ:     c.sampleTime,
			TS64:         c.sampleTime,
			F32:          153.93068,
			F64:          32.02867104,
			LcStr:        "cc578a55-8f57-4475-9355-67dfccac9e8d",
			StrArr:       nil,
			MapStrStr:    nil,
			UUID:         uuid.New(),
			UUIDNullable: uuid.NullUUID{},
		},
		{
			C1:           json.RawMessage(`{"type":"append object as string"}`),
			C2:           42,
			C3:           101,
			C4:           proto.NewNullable(int16(5425)),
			C5:           proto.NewNullable("ok google"),
			Level:        1,
			IPv4:         proto.Null[proto.IPv4](),
			IPv6:         proto.Null[proto.IPv6](),
			TS:           c.sampleTime,
			TSWithTZ:     c.sampleTime,
			TS64:         c.sampleTime,
			F32:          542.1235,
			F64:          0.5555555555555555,
			LcStr:        "cc578a55-8f57-4475-9355-67dfccac9e8d",
			StrArr:       &[]string{},
			MapStrStr:    map[string]string{},
			UUID:         uuid.New(),
			UUIDNullable: uuid.NullUUID{},
		},
	}
}

func (c *Config) Send(t *testing.T) {
	r := require.New(t)

	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	r.NoError(err)
	defer func() {
		_ = file.Close()
	}()

	for i := range c.samples {
		sampleRaw, err := json.Marshal(&c.samples[i])
		r.NoError(err)
		_, err = file.Write(sampleRaw)
		r.NoError(err)
		_, err = file.WriteString("\n")
		r.NoError(err)
	}
	_ = file.Sync()
}

func (c *Config) Validate(t *testing.T) {
	a := assert.New(t)
	r := require.New(t)

	var rows int
	for range 1000 {
		cnt := proto.ColUInt64{}
		err := c.conn.Do(c.ctx, ch.Query{
			Body: `select count(*) from test_table_insert`,
			Result: proto.Results{
				{Name: "count()", Data: &cnt},
			},
		})
		r.NoError(err)

		rows = int(cnt.Row(0))
		if rows != len(c.samples) {
			time.Sleep(time.Millisecond * 10)
			continue
		}
	}

	a.Equal(len(c.samples), rows)

	var (
		c1          = new(proto.ColStr)
		c2          = new(proto.ColInt8)
		c3          = new(proto.ColInt16)
		c4          = new(proto.ColInt16).Nullable()
		c5          = new(proto.ColStr).Nullable()
		level       = new(proto.ColEnum8)
		ipv4        = new(proto.ColIPv4).Nullable()
		ipv6        = new(proto.ColIPv6).Nullable()
		ts          = new(proto.ColDateTime)
		tsWithTz    = new(proto.ColDateTime)
		ts64        = new(proto.ColDateTime64)
		ts64Auto    = new(proto.ColDateTime64)
		ts3339nano  = new(proto.ColDateTime64)
		f32         = new(proto.ColFloat32)
		f64         = new(proto.ColFloat64)
		lcStr       = new(proto.ColStr).LowCardinality()
		strArr      = new(proto.ColStr).Array()
		mapStrStr   = proto.NewMap(new(proto.ColStr), new(proto.ColStr))
		uid         = new(proto.ColUUID)
		uidNullable = proto.NewColNullable[uuid.UUID](new(proto.ColUUID))
	)
	ts64.WithPrecision(proto.PrecisionMilli)
	ts64Auto.WithPrecision(proto.PrecisionNano)

	sampleIdx := 0
	r.NoError(c.conn.Do(c.ctx, ch.Query{
		Body: `select c1, c2, c3, c4, c5, level, ipv4, ipv6, ts, ts_with_tz, ts64, ts64_auto, ts_rfc3339nano, f32, f64, lc_str, str_arr, map_str_str, uuid, uuid_nullable
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
			proto.ResultColumn{Name: "ts_rfc3339nano", Data: ts3339nano},
			proto.ResultColumn{Name: "f32", Data: f32},
			proto.ResultColumn{Name: "f64", Data: f64},
			proto.ResultColumn{Name: "lc_str", Data: lcStr},
			proto.ResultColumn{Name: "str_arr", Data: strArr},
			proto.ResultColumn{Name: "map_str_str", Data: mapStrStr},
			proto.ResultColumn{Name: "uuid", Data: uid},
			proto.ResultColumn{Name: "uuid_nullable", Data: uidNullable},
		},
		OnResult: func(_ context.Context, _ proto.Block) error {
			for i := 0; i < c1.Rows(); i++ {
				sample := c.samples[sampleIdx]
				sampleIdx++

				c1Expected, _ := json.Marshal(sample.C1)
				a.Equal(string(trim(c1Expected)), c1.Row(i))

				a.Equal(sample.C2, c2.Row(i))
				a.Equal(sample.C3, c3.Row(i))
				a.Equal(sample.C4, c4.Row(i))
				a.Equal(sample.C5, c5.Row(i))
				a.Equal(sample.IPv4, ipv4.Row(i))
				a.Equal(sample.IPv6, ipv6.Row(i))
				a.Equal(sample.TS.Unix(), ts.Row(i).Unix())
				a.False(ts3339nano.Row(i).IsZero())
				a.Greater(ts3339nano.Row(0), time.Now().Add(-time.Second*20))
				a.Equal(sample.F32, f32.Row(i))
				a.Equal(sample.F64, f64.Row(i))
				a.Equal(sample.LcStr, lcStr.Row(i))
				a.Equal(sample.UUID, uid.Row(i))
				a.Equal(sample.UUIDNullable.UUID, uidNullable.Row(i).Value)

				if sample.StrArr == nil || len(*sample.StrArr) == 0 {
					a.Equal([]string(nil), strArr.Row(i))
				} else {
					a.Equal(*sample.StrArr, strArr.Row(i))
				}

				if len(sample.MapStrStr) == 0 {
					a.Equal(map[string]string(nil), mapStrStr.Row(i))
				} else {
					a.Equal(sample.MapStrStr, mapStrStr.Row(i))
				}

				a.Equal(sample.TSWithTZ.Unix(), tsWithTz.Row(i).Unix())
				a.Equal("Europe/Moscow", tsWithTz.Row(i).Location().String())

				a.Equal(sample.TS64.UnixMilli()*1e6, ts64.Row(i).UnixNano())
				a.Equal("Local", ts64.Row(i).Location().String())

				a.True(ts64Auto.Row(i).After(sample.TS64), "%s before %s", ts64Auto.Row(i).String(), sample.TS64.String()) // we are use set_time plugin and override this value
				a.Equal("UTC", ts64Auto.Row(i).Location().String())
			}
			return nil
		},
	}))
}

func trim(val []byte) []byte {
	val = bytes.TrimPrefix(val, []byte("\""))
	val = bytes.TrimSuffix(val, []byte("\""))
	return val
}
