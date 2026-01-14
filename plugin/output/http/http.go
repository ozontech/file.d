package http

import (
	"context"
	"encoding/base64"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xhttp"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends events to arbitrary HTTP endpoints. It uses POST requests to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.

Supports [dead queue](/plugin/output/README.md#dead-queue).
}*/

const (
	outPluginType = "http"
)

type Plugin struct {
	config *Config

	client *xhttp.Client

	logger     *zap.Logger
	controller pipeline.OutputPluginController

	batcher      *pipeline.RetriableBatcher
	avgEventSize int

	cancel context.CancelFunc
	mu     *sync.Mutex

	// plugin metrics
	sendErrorMetric *prometheus.CounterVec

	router *pipeline.Router
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of HTTP endpoints in the following format: `SCHEMA://HOST:PORT/PATH`
	Endpoints []string `json:"endpoints" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Content-Type header for HTTP requests.
	ContentType string `json:"content_type" default:"application/json"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use gzip encoding.
	UseGzip bool `json:"use_gzip" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Gzip compression level. Used if `use_gzip=true`.
	GzipCompressionLevel string `json:"gzip_compression_level" default:"default" options:"default|no|best-speed|best-compression|huffman-only"` // *

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
	// > Base64-encoded token for authorization; if set, overrides username/password.
	APIKey string `json:"api_key"` // *

	// > @3@4@5@6
	// > Path or content of a PEM-encoded CA file.
	CACert string `json:"ca_cert"` // *

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
	// > It defines how much time to wait for the connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s" parse:"duration"` // *
	ConnectionTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > A maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` // *
	BatchSize_ int

	// > @3@4@5@6
	// >
	// > A minimum size of events in a batch to send.
	// > If both batch_size and batch_size_bytes are set, they will work together.
	BatchSizeBytes  cfg.Expression `json:"batch_size_bytes" default:"0" parse:"expression"` // *
	BatchSizeBytes_ int

	// > @3@4@5@6
	// >
	// > After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Enable split big batches
	SplitBatch bool `json:"split_batch" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > After a non-retryable write error, fall with a non-zero exit code or not
	Strict bool `json:"strict" default:"false"` // *
}

type KeepAliveConfig struct {
	// Idle keep-alive connections are closed after this duration.
	MaxIdleConnDuration  cfg.Duration `json:"max_idle_conn_duration" parse:"duration" default:"10s"`
	MaxIdleConnDuration_ time.Duration

	// Keep-alive connections are closed after this duration.
	MaxConnDuration  cfg.Duration `json:"max_conn_duration" parse:"duration" default:"5m"`
	MaxConnDuration_ time.Duration
}

type data struct {
	outBuf []byte
	begin  []int
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
	p.logger = params.Logger.Desugar()
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)
	p.mu = &sync.Mutex{}

	p.prepareClient()

	p.logger.Info("starting batcher", zap.Duration("timeout", p.config.BatchFlushTimeout_))

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:        params.PipelineName,
		OutputType:          outPluginType,
		MaintenanceFn:       func(*pipeline.WorkerData) {}, // Empty maintenance function
		Controller:          p.controller,
		Workers:             p.config.WorkersCount_,
		BatchSizeCount:      p.config.BatchSize_,
		BatchSizeBytes:      p.config.BatchSizeBytes_,
		FlushTimeout:        p.config.BatchFlushTimeout_,
		MaintenanceInterval: time.Minute,
		MetricCtl:           params.MetricCtl,
	}

	p.router = params.Router
	backoffOpts := pipeline.BackoffOpts{
		MinRetention:         p.config.Retention_,
		Multiplier:           float64(p.config.RetentionExponentMultiplier),
		AttemptNum:           p.config.Retry,
		IsDeadQueueAvailable: p.router.IsDeadQueueAvailable(),
	}

	onError := func(err error, events []*pipeline.Event) {
		var level zapcore.Level
		if p.config.FatalOnFailedInsert && !p.router.IsDeadQueueAvailable() {
			level = zapcore.FatalLevel
		} else {
			level = zapcore.ErrorLevel
		}

		p.logger.Log(level, "can't send to the http endpoint", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)

		for i := range events {
			p.router.Fail(events[i])
		}
	}

	p.batcher = pipeline.NewRetriableBatcher(
		&batcherOpts,
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

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounterVec("output_http_send_error_total", "Total HTTP send errors", "status_code")
}

func (p *Plugin) prepareClient() {
	config := &xhttp.ClientConfig{
		Endpoints:         p.prepareEndpoints(),
		ConnectionTimeout: p.config.ConnectionTimeout_ * 2,
		AuthHeader:        p.getAuthHeader(),
		KeepAlive: &xhttp.ClientKeepAliveConfig{
			MaxConnDuration:     p.config.KeepAlive.MaxConnDuration_,
			MaxIdleConnDuration: p.config.KeepAlive.MaxIdleConnDuration_,
		},
	}
	if p.config.CACert != "" {
		config.TLS = &xhttp.ClientTLSConfig{
			CACert: p.config.CACert,
		}
	}
	if p.config.UseGzip {
		config.GzipCompressionLevel = p.config.GzipCompressionLevel
	}

	var err error
	p.client, err = xhttp.NewClient(config)
	if err != nil {
		p.logger.Fatal("can't create http client", zap.Error(err))
	}
}

func (p *Plugin) prepareEndpoints() []string {
	res := make([]string, 0, len(p.config.Endpoints))
	for _, e := range p.config.Endpoints {
		// Ensure endpoint doesn't end with slash
		if e[len(e)-1] == '/' {
			e = e[:len(e)-1]
		}
		res = append(res, e)
	}
	return res
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
			begin:  make([]int, 0, p.config.BatchSize_+1),
		}
	}

	data := (*workerData).(*data)
	// handle too much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	eventsCount := 0
	data.begin = data.begin[:0]
	data.outBuf = data.outBuf[:0]
	batch.ForEach(func(event *pipeline.Event) {
		eventsCount++
		data.begin = append(data.begin, len(data.outBuf))
		data.outBuf, _ = event.Encode(data.outBuf)
		data.outBuf = append(data.outBuf, '\n')
	})
	data.begin = append(data.begin, len(data.outBuf))

	var statusCode int
	var err error

	if p.config.SplitBatch {
		statusCode, err = p.sendSplit(0, eventsCount, data.begin, data.outBuf)
	} else {
		statusCode, err = p.send(data.outBuf)
	}

	if err != nil {
		p.sendErrorMetric.WithLabelValues(strconv.Itoa(statusCode)).Inc()
		switch statusCode {
		case http.StatusBadRequest, http.StatusRequestEntityTooLarge:
			const errMsg = "can't send to the http endpoint, non-retryable error occurred"
			fields := []zap.Field{zap.Int("status_code", statusCode), zap.Error(err)}
			if p.config.Strict {
				p.logger.Fatal(errMsg, fields...)
			}
			p.logger.Error(errMsg, fields...)
			return nil
		default:
			p.logger.Error("can't send to the http endpoint, will try other endpoint", zap.Error(err))
			return err
		}
	}

	return nil
}

func (p *Plugin) send(data []byte) (int, error) {
	return p.client.DoTimeout(
		http.MethodPost,
		p.config.ContentType,
		data,
		p.config.ConnectionTimeout_,
		nil,
	)
}

func (p *Plugin) sendSplit(left int, right int, begin []int, data []byte) (int, error) {
	if left == right {
		return http.StatusOK, nil
	}

	statusCode, err := p.client.DoTimeout(
		http.MethodPost,
		p.config.ContentType,
		data[begin[left]:begin[right]],
		p.config.ConnectionTimeout_,
		nil,
	)

	if err != nil {
		p.sendErrorMetric.WithLabelValues(strconv.Itoa(statusCode)).Inc()
		switch statusCode {
		case http.StatusRequestEntityTooLarge:
			// can't send even one log
			if right-left == 1 {
				return statusCode, err
			}

			middle := (left + right) / 2
			statusCode, err = p.sendSplit(left, middle, begin, data)
			if err != nil {
				return statusCode, err
			}

			return p.sendSplit(middle, right, begin, data)
		default:
			return statusCode, err
		}
	}

	return http.StatusOK, nil
}

func (p *Plugin) getAuthHeader() string {
	if p.config.APIKey != "" {
		return "ApiKey " + p.config.APIKey
	}
	if p.config.Username != "" && p.config.Password != "" {
		credentials := []byte(p.config.Username + ":" + p.config.Password)
		return "Basic " + base64.StdEncoding.EncodeToString(credentials)
	}
	return ""
}
