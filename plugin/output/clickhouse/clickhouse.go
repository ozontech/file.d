package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/tls"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
	logger     *zap.Logger
	config     *Config
	batcher    *pipeline.Batcher
	ctx        context.Context
	cancelFunc context.CancelFunc

	query string

	pool Clickhouse

	// plugin metrics

	insertErrorsMetric *prom.CounterVec
	queriesCountMetric *prom.CounterVec
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
	// TODO: allow to set default value
	// TODO: allow to set column aliases to find value in the JSON log
	Name string `json:"name"`
	Type string `json:"type"`
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > TCP Clickhouse address, e.g. 127.0.0.1:9000.
	Address string `json:"address" required:"true"` // *

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
	QuotaKey string `json:"quota_key" default:""` // *

	// > @3@4@5@6
	// >
	// > Clickhouse target table.
	Table string `json:"table" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Clickhouse table columns. Each column must contain `name` and `type`.
	Columns []Column `json:"columns" required:"true"` // *

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
	// > Retries of insertion. If file.d cannot insert for this number of attempts,
	// > file.d will fall with non-zero exit code.
	Retry int `json:"retry" default:"10"` // *

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
	// > Timeout for DB requests in milliseconds.
	DBRequestTimeout  cfg.Duration `json:"db_request_timeout" default:"10s" parse:"duration"` // *
	DBRequestTimeout_ time.Duration

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

func (p *Plugin) RegisterMetrics(ctl *metric.Ctl) {
	p.insertErrorsMetric = ctl.RegisterCounter("output_clickhouse_errors", "Total clickhouse insert errors")
	p.queriesCountMetric = ctl.RegisterCounter("output_clickhouse_queries_count", "How many queries sent by clickhouse output plugin")
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.logger = params.Logger.Desugar()
	p.config = config.(*Config)
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())

	if p.config.Retry < 1 {
		p.logger.Fatal("'retry' can't be <1")
	}
	if p.config.Retention_ < 1 {
		p.logger.Fatal("'retention' can't be <1")
	}
	if p.config.DBRequestTimeout_ < 1 {
		p.logger.Fatal("'db_request_timeout' can't be <1")
	}
	if _, err := inferInsaneColInputs(p.config.Columns); err != nil {
		p.logger.Fatal("invalid database schema", zap.Error(err))
	}

	p.query = fmt.Sprintf("INSERT INTO %s VALUES", p.config.Table)

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

	var b tls.ConfigBuilder
	if p.config.CACert != "" {
		b := tls.NewConfigBuilder()
		err := b.AppendCARoot(p.config.CACert)
		if err != nil {
			p.logger.Fatal("can't append CA root", zap.Error(err))
		}
	}

	var err error
	p.pool, err = chpool.Dial(p.ctx, chpool.Options{
		ClientOptions: ch.Options{
			Logger:           p.logger.Named("driver"),
			Address:          p.config.Address,
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
		MaxConns:          int32(p.config.WorkersCount_),
		MinConns:          int32(p.config.WorkersCount_),
		HealthCheckPeriod: p.config.HealthCheckPeriod_,
	})
	if err != nil {
		p.logger.Fatal("create clickhouse connection pool: %s", zap.Error(err))
	}

	p.batcher = pipeline.NewBatcher(pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		OutFn:          p.out,
		Controller:     params.Controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
	})

	p.batcher.Start(p.ctx)
}

func (p *Plugin) Stop() {
	p.cancelFunc()
	p.batcher.Stop()
	p.pool.Close()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		// we don't check the error, schema already validated in the Start
		schema, _ := inferInsaneColInputs(p.config.Columns)
		*workerData = schema
	}

	input := (*workerData).([]InsaneColumn)
	for i := range input {
		input[i].ColInput.Reset()
	}

	for _, event := range batch.Events {
		for _, col := range input {
			node, _ := event.Root.DigStrict(col.Name)
			if err := col.ColInput.Append(node); err != nil {
				// TODO: handle case when we can't append in the batch, e.g. columns is not nullable, but value it is
				p.logger.Fatal("can't append value in the batch", zap.Error(err))
			}
		}
	}

	queryInput := make(proto.Input, len(input))
	for i := range input {
		queryInput[i] = proto.InputColumn{
			Name: input[i].Name,
			Data: input[i].ColInput,
		}
	}

	var err error
	for i := 0; i < p.config.Retry; i++ {
		err = p.do(queryInput)
		if err == nil {
			break
		}
		p.insertErrorsMetric.WithLabelValues().Inc()
		time.Sleep(p.config.Retention_)
		p.logger.Error("an attempt to insert a batch failed", zap.Error(err))
	}
	if err != nil {
		p.logger.Fatal("can't insert to the table", zap.Error(err),
			zap.Int("retries", p.config.Retry),
			zap.String("table", p.config.Table))
	}
}

func (p *Plugin) do(queryInput proto.Input) error {
	defer p.queriesCountMetric.WithLabelValues().Inc()

	ctx, cancel := context.WithTimeout(p.ctx, p.config.DBRequestTimeout_)
	defer cancel()

	return p.pool.Do(ctx, ch.Query{
		Body:  p.query,
		Input: queryInput,
	})
}
