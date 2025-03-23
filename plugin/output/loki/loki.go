package loki

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xhttp"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends the logs batches to Loki using HTTP API.
}*/

var errUnixNanoFormat = errors.New("please send time in UnixNano format or add a convert_date action")

const (
	outPluginType = "loki"
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
	// > @3@4@5@6
	// >
	// > A full URI address of Loki
	// >
	// > Example address
	// >
	// > http://127.0.0.1:3100 or https://loki:3100
	Address string `json:"address" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Array of labels to send logs
	// >
	// > Example labels
	// >
	// > label=value
	Labels []Label `json:"labels" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > Message field from log to be mapped to loki
	// >
	// > Example
	// >
	// > message
	MessageField string `json:"message_field" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Timestamp field from log to be mapped to loki
	// >
	// > Example
	// >
	// > timestamp
	TimestampField string `json:"timestamp_field" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Auth config.
	// > Disabled by default.
	// > See AuthConfig for details.
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
	// > @3@4@5@6
	// >
	// > AuthStrategy.Strategy describes strategy to use.
	Strategy  string `json:"strategy" default:"disabled" options:"disabled|tenant|basic|bearer"` // *
	Strategy_ AuthStrategy

	// > @3@4@5@6
	// >
	// > TenantID for Tenant Authentication.
	TenantID string `json:"tenant_id"` // *

	// > @3@4@5@6
	// >
	// > Username for HTTP Basic Authentication.
	Username string `json:"username"` // *

	// > @3@4@5@6
	// >
	// > Password for HTTP Basic Authentication.
	Password string `json:"password"` // *

	// > @3@4@5@6
	// >
	// > Token for HTTP Bearer Authentication.
	BearerToken string // *
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
	cancel       context.CancelFunc

	client  *xhttp.Client
	batcher *pipeline.RetriableBatcher

	// plugin metrics
	sendErrorMetric *prometheus.CounterVec

	labels            map[string]string
	nameByBearerToken map[string]string
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

	p.labels = p.parseLabels()

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
	p.cancel = cancel

	p.batcher.Start(ctx)
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
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

	dataArr := root.AddFieldNoAlloc(root, "data").MutateToArray()
	batch.ForEach(func(event *pipeline.Event) {
		dataArr.AddElementNoAlloc(root).MutateToNode(event.Root.Node)
	})

	data.outBuf = root.Encode(data.outBuf)

	insaneJSON.Release(root)

	code, err := p.send(context.Background(), data.outBuf)
	if err != nil {
		p.sendErrorMetric.WithLabelValues(strconv.Itoa(code)).Inc()
		p.logger.Error("can't send data to Loki", zap.String("address", p.config.Address), zap.Error(err))

		// skip retries for bad request or time format errors
		if code == http.StatusBadRequest || errors.Is(err, errUnixNanoFormat) {
			return nil
		}
	} else {
		p.logger.Debug("successfully sent", zap.String("data", string(data.outBuf)))
	}

	return err
}

type stream struct {
	StreamLabels map[string]string `json:"stream"`
	Values       [][]any           `json:"values"`
}

type request struct {
	Streams []stream `json:"streams"`
}

func (p *Plugin) send(ctx context.Context, data []byte) (int, error) {
	root := insaneJSON.Spawn()
	if err := root.DecodeBytes(data); err != nil {
		p.logger.Error("failed to decode json", zap.Error(err))
		return 0, err
	}

	messages := root.Dig("data").AsArray()
	values := make([][]any, 0, len(messages))

	for _, msg := range messages {
		ts := msg.Dig(p.config.TimestampField).AsString()
		msg.Dig(p.config.TimestampField).Suicide()

		if ts == "" {
			ts = fmt.Sprintf(`%d`, time.Now().UnixNano())
		} else if !p.isUnixNanoFormat(ts) {
			return 0, errUnixNanoFormat
		}

		logMsg := msg.Dig(p.config.MessageField).AsString()
		msg.Dig(p.config.MessageField).Suicide()

		logLine := []any{
			ts,
			logMsg,
			json.RawMessage(msg.EncodeToString()),
		}

		values = append(values, logLine)
	}

	output := request{
		Streams: []stream{
			{
				StreamLabels: p.labels,
				Values:       values,
			},
		},
	}

	data, err := json.Marshal(output)
	if err != nil {
		return 0, err
	}

	p.logger.Debug("sent", zap.String("data", string(data)))

	statusCode, err := p.client.DoTimeout(
		http.MethodPost,
		"application/json",
		data,
		p.config.ConnectionTimeout_,
		nil,
	)
	if statusCode != http.StatusNoContent {
		return statusCode, fmt.Errorf("bad response: code=%d, err=%v", statusCode, err)
	}

	return statusCode, nil
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounterVec("output_loki_send_error", "Total Loki send errors", "status_code")
}

func (p *Plugin) prepareClient() {
	config := &xhttp.ClientConfig{
		Endpoints:         []string{fmt.Sprintf("%s/loki/api/v1/push", p.config.Address)},
		ConnectionTimeout: p.config.ConnectionTimeout_ * 2,
		AuthHeader:        p.getAuthHeader(),
		CustomHeaders:     p.getCustomHeaders(),
		KeepAlive: &xhttp.ClientKeepAliveConfig{
			MaxConnDuration:     p.config.KeepAlive.MaxConnDuration_,
			MaxIdleConnDuration: p.config.KeepAlive.MaxIdleConnDuration_,
		},
	}

	var err error
	p.client, err = xhttp.NewClient(config)
	if err != nil {
		p.logger.Fatal("can't create http client", zap.Error(err))
	}
}

func (p *Plugin) getCustomHeaders() map[string]string {
	headers := make(map[string]string)

	if p.config.Auth.Strategy_ == StrategyTenant {
		headers["X-Scope-OrgID"] = p.config.Auth.TenantID
	}

	return headers
}

func (p *Plugin) parseLabels() map[string]string {
	labels := make(map[string]string, len(p.config.Labels))

	for _, v := range p.config.Labels {
		labels[v.Label] = v.Value
	}

	return labels
}

func (p *Plugin) isUnixNanoFormat(ts string) bool {
	nano, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return false
	}

	t := time.Unix(0, nano)

	minTime := time.Unix(0, 0)
	maxTime := time.Now()

	return t.After(minTime) && t.Before(maxTime)
}

func (p *Plugin) getAuthHeader() string {
	if p.config.Auth.Strategy_ == StrategyBasic {
		credentials := []byte(p.config.Auth.Username + ":" + p.config.Auth.Password)
		return fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(credentials))

	}

	if p.config.Auth.Strategy_ == StrategyBearer {
		return fmt.Sprintf("Bearer %s", p.config.Auth.BearerToken)
	}

	return ""
}
