package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtls"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends the event batches to Clickhouse database using
[Native format](https://clickhouse.com/docs/en/interfaces/formats/#native) and
[Native protocol](https://clickhouse.com/docs/en/interfaces/tcp/).

File.d uses low level Go client - [ch-go](https://github.com/ClickHouse/ch-go) to provide these features.
}*/

const (
	outPluginType = "clickhouse"
)

type Clickhouse interface {
	Close()
	Do(ctx context.Context, query ch.Query) error
}

type Plugin struct {
	logger *zap.Logger

	config     *Config
	batcher    *pipeline.RetriableBatcher
	ctx        context.Context
	cancelFunc context.CancelFunc

	query string

	// TODO: support shards
	instances []Clickhouse
	requestID atomic.Int64

	// plugin metrics
	insertErrorsMetric prometheus.Counter
	queriesCountMetric prometheus.Counter
}

type Setting struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Important bool   `json:"important"`
}

type Settings []Setting

func (s Settings) toProtoSettings() []ch.Setting {
	result := make([]ch.Setting, 0, len(s))
	for _, setting := range s {
		result = append(result, ch.Setting{
			Key:       setting.Key,
			Value:     setting.Value,
			Important: setting.Important,
		})
	}
	return result
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type InsertStrategy byte

const (
	StrategyRoundRobin InsertStrategy = iota
	StrategyInOrder
)

type Address struct {
	Addr   string `json:"addr"`
	Weight int    `json:"weight"`
}

func (a *Address) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return nil
	}

	switch b[0] {
	case '"':
		a.Weight = 1
		return json.Unmarshal(b, &a.Addr)
	case '{':
		type tmpAddress Address
		tmp := tmpAddress{}
		dec := json.NewDecoder(bytes.NewReader(b))
		dec.DisallowUnknownFields()
		err := dec.Decode(&tmp)
		*a = Address(tmp)
		return err
	default:
		return errors.New("failed to unmarshal to Address, the value must be string or object")
	}
}

var _ json.Unmarshaler = (*Address)(nil)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > TCP Clickhouse addresses, e.g.: 127.0.0.1:9000.
	// > Check the insert_strategy to find out how File.d will behave with a list of addresses.
	// >
	// > Accepts strings or objects, e.g.:
	// > ```yaml
	// > addresses:
	// >   - 127.0.0.1:9000 # the same as {addr:'127.0.0.1:9000',weight:1}
	// >   - addr: 127.0.0.1:9001
	// >     weight: 2
	// > ```
	// >
	// > When some addresses get weight greater than 1 and round_robin insert strategy is used,
	// > it works as classical weighted round robin. Given {(a_1,w_1),(a_1,w_1),...,{a_n,w_n}},
	// > where a_i is the ith address and w_i is the ith address' weight, requests are sent in order:
	// > w_1 times to a_1, w_2 times to a_2, ..., w_n times to a_n, w_1 times to a_1 and so on.
	Addresses []Address `json:"addresses" required:"true" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > If more than one addresses are set, File.d will insert batches depends on the strategy:
	// > round_robin - File.d will send requests in the round-robin order.
	// > in_order - File.d will send requests starting from the first address, ending with the number of retries.
	InsertStrategy  string `json:"insert_strategy" default:"round_robin" options:"round_robin|in_order"` // *
	InsertStrategy_ InsertStrategy

	// > @3@4@5@6
	// >
	// > CA certificate in PEM encoding. This can be a path or the content of the certificate.
	CACert string `json:"ca_cert" default:""` // *

	// > @3@4@5@6
	// >
	// > Clickhouse database name to search the table.
	Database string `json:"database" default:"default"` // *

	// > @3@4@5@6
	// >
	// > Clickhouse database user.
	User string `json:"user" default:"default"` // *

	// > @3@4@5@6
	// >
	// > Clickhouse database password.
	Password string `json:"password" default:""` // *

	// > @3@4@5@6
	// >
	// > Clickhouse quota key.
	// > https://clickhouse.com/docs/en/operations/quotas
	QuotaKey string `json:"quota_key" default:""` // *

	// > @3@4@5@6
	// >
	// > Clickhouse target table.
	Table string `json:"table" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Clickhouse table columns. Each column must contain `name` and `type`.
	// > File.d supports next data types:
	// > * Signed and unsigned integers from 8 to 64 bits.
	// > If you set 128-256 bits - File.d will cast the number to the int64.
	// > * DateTime, DateTime64
	// > * String
	// > * Enum8, Enum16
	// > * Bool
	// > * Nullable
	// > * IPv4, IPv6
	// > * LowCardinality(String)
	// > * Array(String)
	// >
	// > If you need more types, please, create an issue.
	Columns []Column `json:"columns" required:"true"` // *

	// > @3@4@5@6
	// >
	// > If true, file.d fails when types are mismatched.
	// >
	// > If false, file.d will cast any JSON type to the column type.
	// >
	// > For example, if strict_types is false and an event value is a Number,
	// > but the column type is a Bool, the Number will be converted to the "true"
	// > if the value is "1".
	// > But if the value is an Object and the column is an Int
	// > File.d converts the Object to "0" to prevent fall.
	// >
	// > In the non-strict mode, for String and Array(String) columns the value will be encoded to JSON.
	// >
	// > If the strict mode is enabled file.d fails (exit with code 1) in above examples.
	StrictTypes bool `json:"strict_types" default:"false"` // *

	// > @3@4@5@6
	// >
	// > The level of the Compression.
	// > Disabled - lowest CPU overhead.
	// > LZ4 - medium CPU overhead.
	// > ZSTD - high CPU overhead.
	// > None - uses no compression but data has checksums.
	Compression string `default:"disabled" options:"disabled|lz4|zstd|none"` // *

	// > @3@4@5@6
	// >
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not
	// > **Experimental feature**
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Additional settings to the Clickhouse.
	// > Settings list: https://clickhouse.com/docs/en/operations/settings/settings
	ClickhouseSettings Settings `json:"clickhouse_settings"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > Timeout for each insert request.
	InsertTimeout  cfg.Duration `json:"insert_timeout" default:"10s" parse:"duration"` // *
	InsertTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Max connections in the connection pool.
	MaxConns  cfg.Expression `json:"max_conns" default:"gomaxprocs*4"  parse:"expression"` // *
	MaxConns_ int32

	// > @3@4@5@6
	// >
	// > Min connections in the connection pool.
	MinConns  cfg.Expression `json:"min_conns" default:"gomaxprocs*1"  parse:"expression"` // *
	MinConns_ int32

	// > @3@4@5@6
	// >
	// > How long a connection lives before it is killed and recreated.
	MaxConnLifetime  cfg.Duration `json:"max_conn_lifetime" default:"30m" parse:"duration"` // *
	MaxConnLifetime_ time.Duration

	// > @3@4@5@6
	// >
	// > How long an unused connection lives before it is killed.
	MaxConnIdleTime  cfg.Duration `json:"max_conn_idle_time" default:"5m" parse:"duration"` // *
	MaxConnIdleTime_ time.Duration

	// > @3@4@5@6
	// >
	// > How often to check that idle connections is time to kill.
	HealthCheckPeriod  cfg.Duration `json:"health_check_period" default:"1m" parse:"duration"` // *
	HealthCheckPeriod_ time.Duration

	// > @3@4@5@6
	// >
	// > How much workers will be instantiated to send batches.
	// > It also configures the amount of minimum and maximum number of database connections.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4"  parse:"expression"` // *
	BatchSize_ int

	// > @3@4@5@6
	// >
	// > A minimum size of events in a batch to send.
	// > If both batch_size and batch_size_bytes are set, they will work together.
	BatchSizeBytes  cfg.Expression `json:"batch_size_bytes" default:"0" parse:"expression"` // *
	BatchSizeBytes_ int

	// > @3@4@5@6
	// >
	// > After this timeout batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.insertErrorsMetric = ctl.RegisterCounter("output_clickhouse_errors", "Total clickhouse insert errors")
	p.queriesCountMetric = ctl.RegisterCounter("output_clickhouse_queries_count", "How many queries sent by clickhouse output plugin")
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.logger = params.Logger.Desugar()

	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())

	if p.config.Retention_ < 1 {
		p.logger.Fatal("'retention' can't be <1")
	}
	if p.config.InsertTimeout_ < 1 {
		p.logger.Fatal("'db_request_timeout' can't be <1")
	}

	schema, err := inferInsaneColInputs(p.config.Columns)
	if err != nil {
		p.logger.Fatal("invalid database schema", zap.Error(err))
	}
	input := inputFromColumns(schema)
	p.query = input.Into(p.config.Table)

	switch p.config.InsertStrategy {
	case "round_robin":
		p.config.InsertStrategy_ = StrategyRoundRobin
	case "in_order":
		p.config.InsertStrategy_ = StrategyInOrder
	}

	var compression ch.Compression
	switch strings.ToLower(p.config.Compression) {
	default:
		fallthrough
	case "disabled":
		compression = ch.CompressionDisabled
	case "lz4":
		compression = ch.CompressionLZ4
	case "zstd":
		compression = ch.CompressionZSTD
	case "none":
		compression = ch.CompressionNone
	}

	var b xtls.ConfigBuilder
	if p.config.CACert != "" {
		b := xtls.NewConfigBuilder()
		err := b.AppendCARoot(p.config.CACert)
		if err != nil {
			p.logger.Fatal("can't append CA root", zap.Error(err))
		}
	}

	for _, addr := range p.config.Addresses {
		addr.Addr = addrWithDefaultPort(addr.Addr, "9000")
		pool, err := chpool.New(p.ctx, chpool.Options{
			ClientOptions: ch.Options{
				Logger:           p.logger.Named("driver"),
				Address:          addr.Addr,
				Database:         p.config.Database,
				User:             p.config.User,
				Password:         p.config.Password,
				QuotaKey:         p.config.QuotaKey,
				Compression:      compression,
				Settings:         p.config.ClickhouseSettings.toProtoSettings(),
				DialTimeout:      time.Second * 10,
				TLS:              b.Build(),
				HandshakeTimeout: time.Minute,
			},
			MaxConnLifetime:   p.config.MaxConnLifetime_,
			MaxConnIdleTime:   p.config.MaxConnIdleTime_,
			MaxConns:          p.config.MaxConns_,
			MinConns:          p.config.MinConns_,
			HealthCheckPeriod: p.config.HealthCheckPeriod_,
		})
		if err != nil {
			p.logger.Fatal("create clickhouse connection pool", zap.Error(err), zap.String("addr", addr.Addr))
		}
		for j := 0; j < addr.Weight; j++ {
			p.instances = append(p.instances, pool)
		}
	}

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		Controller:     params.Controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
		MetricCtl:      params.MetricCtl,
	}

	backoffOpts := pipeline.BackoffOpts{
		MinRetention: p.config.Retention_,
		Multiplier:   float64(p.config.RetentionExponentMultiplier),
		AttemptNum:   p.config.Retry,
	}

	onError := func(err error) {
		var level zapcore.Level
		if p.config.FatalOnFailedInsert {
			level = zapcore.FatalLevel
		} else {
			level = zapcore.ErrorLevel
		}

		p.logger.Log(level, "can't insert to the table", zap.Error(err),
			zap.Int("retries", p.config.Retry),
			zap.String("table", p.config.Table))
	}

	p.batcher = pipeline.NewRetriableBatcher(
		&batcherOpts,
		p.out,
		backoffOpts,
		onError,
	)

	p.batcher.Start(p.ctx)
}

func (p *Plugin) Stop() {
	p.cancelFunc()
	p.batcher.Stop()
	for _, clickhouse := range p.instances {
		clickhouse.Close()
	}
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

type data struct {
	cols  []InsaneColumn
	input proto.Input
}

func (d data) reset() {
	for i := range d.cols {
		d.cols[i].ColInput.Reset()
	}
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		// we don't check the error, schema already validated in the Start
		columns, _ := inferInsaneColInputs(p.config.Columns)
		input := inputFromColumns(columns)
		*workerData = data{
			cols:  columns,
			input: input,
		}
	}

	data := (*workerData).(data)
	data.reset()

	batch.ForEach(func(event *pipeline.Event) {
		for _, col := range data.cols {
			node := event.Root.Dig(col.Name)

			var insaneNode InsaneNode
			if node != nil && p.config.StrictTypes {
				insaneNode = StrictNode{node.MutateToStrict()}
			} else if node != nil {
				insaneNode = NonStrictNode{node}
			}

			if err := col.ColInput.Append(insaneNode); err != nil {
				// we can't append the value to the column because of the node has wrong format,
				// so append zero value
				err := col.ColInput.Append(ZeroValueNode{})
				if err != nil {
					p.logger.Fatal("why err isn't nil?",
						zap.Error(err),
						zap.String("column", col.Name),
						zap.Any("event", json.RawMessage(event.Root.EncodeToByte())),
					)
				}
			}
		}
	})

	var err error
	for i := range p.instances {
		requestID := p.requestID.Inc()
		clickhouse := p.getInstance(requestID, i)
		err = p.do(clickhouse, data.input)
		if err == nil {
			return nil
		}
	}
	if err != nil {
		p.insertErrorsMetric.Inc()
		p.logger.Error(
			"an attempt to insert a batch failed",
			zap.Error(err),
		)
	}

	return err
}

func (p *Plugin) do(clickhouse Clickhouse, queryInput proto.Input) error {
	defer p.queriesCountMetric.Inc()

	ctx, cancel := context.WithTimeout(p.ctx, p.config.InsertTimeout_)
	defer cancel()

	return clickhouse.Do(ctx, ch.Query{
		Body:  p.query,
		Input: queryInput,
	})
}

func (p *Plugin) getInstance(requestID int64, retry int) Clickhouse {
	var instanceIdx int
	switch p.config.InsertStrategy_ {
	case StrategyInOrder:
		instanceIdx = retry % len(p.instances)
	case StrategyRoundRobin:
		instanceIdx = int(requestID) % len(p.instances)
	}
	return p.instances[instanceIdx]
}

func addrWithDefaultPort(addr string, defaultPort string) string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		// port isn't specified
		return net.JoinHostPort(addr, defaultPort)
	}
	if port == "" {
		return addr + defaultPort
	}
	return addr
}
