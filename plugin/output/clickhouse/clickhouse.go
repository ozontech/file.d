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
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/tls"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
It sends the event batches to clickhouse db using clickhouse-go.
}*/

const (
	outPluginType = "clickhouse"
)

type Clickhouse interface {
	Ping(ctx context.Context) error
	Close()
	Do(ctx context.Context, query ch.Query) error
}

type Plugin struct {
	logger     *zap.SugaredLogger
	config     *Config
	batcher    *pipeline.Batcher
	ctx        context.Context
	cancelFunc context.CancelFunc

	avgEventSize int
	pool         Clickhouse

	// plugin metrics

	execErrorsMetric   *prom.CounterVec
	writtenEventMetric *prom.CounterVec
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

type Schema struct {
	Columns []Column `json:"columns"`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > ClickhouseSQL connection string in DSN format.
	// >
	// > Example DSN:
	// >
	// > `clickhouse://username:password@host1:9000,host2:9000/database?dial_timeout=200ms&max_execution_time=60&compress=true`
	// >
	// > Read more about the query values in the doc: https://github.com/ClickHouse/clickhouse-go#dsn
	Address string `json:"address" required:"true"` // *

	Database string

	// > @3@4@5@6
	// >
	// > CA certificate in PEM encoding. This can be a path or the content of the certificate.
	// > If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.
	CACert string `json:"ca_cert" default:""` // *

	// > @3@4@5@6
	// >
	// > Clickhouse target table.
	Table string `json:"table" required:"true"` // *

	Schema Schema `json:"schema"`

	Compression string `default:"none" options:"disabled|lz4|zstd|none"`

	// > @3@4@5@6
	// >
	// > Retries of insertion.
	Retry int `json:"retry" default:"5"` // *
	// > @3@4@5@6
	// >
	// > Allowing Clickhouse to discard extra data.
	// > If disabled and extra data found, Clickhouse throws an error and file.d will infinitely retry invalid requests.
	// > If you want to disable the settings, check the `keep_fields` plugin to prevent the appearance of extra data.
	SkipUnknownFields bool `json:"skip_unknown_fields" default:"true"`

	// > @3@4@5@6
	// >
	// > Additional settings to the Clickhouse.
	// > All settings list: https://clickhouse.com/docs/en/operations/settings/settings
	ClickhouseSettings Settings

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Timeout for DB requests in milliseconds.
	DBRequestTimeout  cfg.Duration `json:"db_request_timeout" default:"3000ms" parse:"duration"` // *
	DBRequestTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > How much workers will be instantiated to send batches.
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
	p.execErrorsMetric = ctl.RegisterCounter("output_clickhouse_errors", "Total clickhouse exec errors")
	p.writtenEventMetric = ctl.RegisterCounter("output_clickhouse_event_written", "Total events written to ???")
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.logger = params.Logger
	p.config = config.(*Config)
	p.ctx, p.cancelFunc = context.WithCancel(context.Background())
	p.avgEventSize = params.PipelineSettings.AvgEventSize

	if p.config.Retry < 1 {
		p.logger.Fatal("'retry' can't be <1")
	}
	if p.config.Retention_ < 1 {
		p.logger.Fatal("'retention' can't be <1")
	}
	if p.config.DBRequestTimeout_ < 1 {
		p.logger.Fatal("'db_request_timeout' can't be <1")
	}
	if _, err := inferInsaneColInputs(p.config.Schema); err != nil {
		p.logger.Fatalf("invalid database schema: %s", err)
	}

	{
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
				p.logger.Fatalf("can't append CA root: %s", err.Error())
			}
		}

		var err error
		p.pool, err = chpool.Dial(p.ctx, chpool.Options{
			ClientOptions: ch.Options{
				Logger:                       p.logger.Desugar(),
				Address:                      p.config.Address,
				Database:                     p.config.Database,
				User:                         "",
				Password:                     "",
				QuotaKey:                     "",
				Compression:                  compression,
				Settings:                     p.config.ClickhouseSettings.toProtoSettings(),
				Dialer:                       nil,
				DialTimeout:                  time.Second * 10,
				TLS:                          b.Build(),
				ProtocolVersion:              0,
				HandshakeTimeout:             time.Second * 10,
				OpenTelemetryInstrumentation: false,
				TracerProvider:               nil,
				MeterProvider:                nil,
			},
			MaxConnLifetime:   0,
			MaxConnIdleTime:   0,
			MaxConns:          0,
			MinConns:          0,
			HealthCheckPeriod: 0,
		})
		if err != nil {
			p.logger.Fatalf("create clickhouse pool: %s", err)
		}
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
		schema, _ := inferInsaneColInputs(p.config.Schema)
		*workerData = schema
	}

	input := (*workerData).([]InsaneColumn)
	for i := range input {
		input[i].ColInput.Reset()
	}

	for _, event := range batch.Events {
		for _, col := range input {
			node, err := event.Root.DigStrict(col.Name)
			if err != nil {
				continue
			}

			if err := col.ColInput.Append(node); err != nil {
				logger.Warnf("append: %s", err)
			}
		}
	}

	var queryInput proto.Input
	for i := range input {
		queryInput = append(queryInput, proto.InputColumn{
			Name: input[i].Name,
			Data: input[i].ColInput,
		})
	}
	err := p.pool.Do(p.ctx, ch.Query{
		Body:  fmt.Sprintf("INSERT INTO %s VALUES", p.config.Table),
		Input: queryInput,
	})
	if err != nil {
		logger.Fatalf("do: %s", err.Error())
	}
}
