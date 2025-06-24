package prometheus

import (
	"context"
	"strings"
	"time"

	"github.com/castai/promwrite"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends the logs batches to Loki using HTTP API.
}*/

const (
	outPluginType = "prometheus"
)

type data struct {
	outBuf []byte
}

type Label struct {
	Label string `json:"label" required:"true"`
	Value string `json:"value" required:"true"`
}

// ! config-params
// ^ config-params
type Config struct {
	Endpoint string `json:"endpoint" required:"true" default:"http://localhost:9090/api/v1/write"` // *

	// > @3@4@5@6
	// >
	// > Auth config.
	// >
	// > `AuthConfig` params:
	// > * `strategy` describes strategy to use; options:"disabled|tenant|basic|bearer"
	// > By default strategy is `disabled`.
	// > * `tenant_id` should be provided if strategy is `tenant`.
	// > * `username` should be provided if strategy is `basic`.
	// > Username is used for HTTP Basic Authentication.
	// > * `password` should be provided if strategy is `basic`.
	// > Password is used for HTTP Basic Authentication.
	// > * `bearer_token` should be provided if strategy is `bearer`.
	// > Token is used for HTTP Bearer Authentication.
	Auth AuthConfig `json:"auth" child:"true"` // *

	// > @3@4@5@6
	// >
	// > If set true, the plugin will use SSL/TLS connections method.
	TLSEnabled bool `json:"tls_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will skip SSL/TLS verification.
	TLSSkipVerify bool `json:"tls_skip_verify" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Client timeout when sends requests to Loki HTTP API.
	RequestTimeout  cfg.Duration `json:"request_timeout" default:"1s" parse:"duration"` // *
	RequestTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how much time to wait for the connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s" parse:"duration"` // *
	ConnectionTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Keep-alive config.
	// >
	// > `KeepAliveConfig` params:
	// > * `max_idle_conn_duration` - idle keep-alive connections are closed after this duration.
	// > By default idle connections are closed after `10s`.
	// > * `max_conn_duration` - keep-alive connections are closed after this duration.
	// > If set to `0` - connection duration is unlimited.
	// > By default connection duration is `5m`.
	KeepAlive KeepAliveConfig `json:"keep_alive" child:"true"` // *

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

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to Loki.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

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
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *
}

type AuthStrategy byte

const (
	StrategyDisabled AuthStrategy = iota
	StrategyTenant
	StrategyBasic
	StrategyBearer
)

// ! config-params
// ^ config-params
type AuthConfig struct {
	// > AuthStrategy.Strategy describes strategy to use.
	Strategy  string `json:"strategy" default:"disabled" options:"disabled|tenant|basic|bearer"`
	Strategy_ AuthStrategy

	// > TenantID for Tenant Authentication.
	TenantID string `json:"tenant_id"`

	// > Username for HTTP Basic Authentication.
	Username string `json:"username"`

	// > Password for HTTP Basic Authentication.
	Password string `json:"password"`

	// > Token for HTTP Bearer Authentication.
	BearerToken string `json:"bearer_token"`
}

type KeepAliveConfig struct {
	// Idle keep-alive connections are closed after this duration.
	MaxIdleConnDuration  cfg.Duration `json:"max_idle_conn_duration" parse:"duration" default:"10s"`
	MaxIdleConnDuration_ time.Duration

	// Keep-alive connections are closed after this duration.
	MaxConnDuration  cfg.Duration `json:"max_conn_duration" parse:"duration" default:"5m"`
	MaxConnDuration_ time.Duration
}

type Plugin struct {
	controller   pipeline.OutputPluginController
	logger       *zap.Logger
	config       *Config
	avgEventSize int

	ctx    context.Context
	cancel context.CancelFunc

	client  *promwrite.Client
	batcher *pipeline.RetriableBatcher

	// plugin metrics
	sendErrorMetric *prometheus.CounterVec

	collector *metricCollector
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.registerMetrics(params.MetricCtl)
	p.collector = newCollector(10*time.Second, p, p.logger)

	p.prepareClient()

	batcherOpts := &pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		Controller:     p.controller,
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

		p.logger.Log(level, "can't send data to loki", zap.Error(err),
			zap.Int("retries", p.config.Retry))
	}

	p.batcher = pipeline.NewRetriableBatcher(
		batcherOpts,
		p.out,
		backoffOpts,
		onError,
	)

	ctx, cancel := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = cancel

	p.batcher.Start(ctx)
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
	p.collector.shutdown()
	p.cancel()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}

	data := (*workerData).(*data)

	// handle too much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	data.outBuf = data.outBuf[:0]

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()
	batch.ForEach(func(event *pipeline.Event) {
		dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
	})

	err := p.send(root)
	if err != nil {
		p.sendErrorMetric.WithLabelValues().Inc()
		p.logger.Error("can't send data to Loki", zap.String("address", p.config.Endpoint), zap.Error(err))
	} else {
		p.logger.Debug("successfully sent", zap.String("data", string(data.outBuf)))
	}

	return err
}

func (p *Plugin) send(root *insaneJSON.Root) error {
	messages := root.Dig("data").AsArray()
	// # {"name":"partitions_total","labels":{"partition":"1"},"timestamp":"2025-06-05T05:19:28.129666195Z","value": 1}}
	values := make([]promwrite.TimeSeries, 0, len(messages))

	for _, msg := range messages {
		nameNode := msg.Dig("name")
		name := nameNode.AsString()
		nameNode.Suicide()

		typeNode := msg.Dig("type")
		metricType := typeNode.AsString()
		typeNode.Suicide()

		timestampNode := msg.Dig("timestamp")
		timestamp := timestampNode.AsInt64()
		timestampNode.Suicide()

		valueNode := msg.Dig("value")
		value := valueNode.AsFloat()
		valueNode.Suicide()

		labelsNode := msg.Dig("labels")
		labelValues := labelsNode.AsFields()
		labelsNode.Suicide()

		labels := make([]promwrite.Label, 0, len(labelValues)+1)
		labels = append(labels, promwrite.Label{
			Name:  "__name__",
			Value: name,
		})
		for _, l := range labelValues {
			labels = append(labels, promwrite.Label{
				Name:  l.AsString(),
				Value: labelsNode.Dig(l.AsString()).AsString(),
			})
		}

		values = append(values, p.collector.handleMetric(
			labels,
			value,
			timestamp,
			metricType,
		)...)
	}

	err := p.sendToStorage(values)
	if err != nil {
		// TODO: add metrics
		if strings.Contains(err.Error(), "out of order sample") || strings.Contains(err.Error(), "duplicate sample for") {
			p.logger.Warn("can't send data to Prometheus", zap.Error(err))
			return nil
		}
	}

	return err
}

func (p *Plugin) sendToStorage(values []promwrite.TimeSeries) error {
	_, err := p.client.Write(context.Background(), &promwrite.WriteRequest{TimeSeries: values})
	return err
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounterVec("output_loki_send_error", "Total Loki send errors")
}

func (p *Plugin) prepareClient() {
	p.client = promwrite.NewClient(p.config.Endpoint)
}
