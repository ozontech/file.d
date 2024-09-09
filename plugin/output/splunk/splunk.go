// Package splunk is an output plugin that sends events to splunk database.
package splunk

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends events to splunk.
}*/

const (
	outPluginType = "splunk"

	gzipContentEncoding = "gzip"
)

type gzipCompressionLevel int

const (
	gzipCompressionLevelDefault gzipCompressionLevel = iota
	gzipCompressionLevelNo
	gzipCompressionLevelBestSpeed
	gzipCompressionLevelBestCompression
	gzipCompressionLevelHuffmanOnly
)

func (l gzipCompressionLevel) toFastHTTP() int {
	switch l {
	case gzipCompressionLevelNo:
		return fasthttp.CompressNoCompression
	case gzipCompressionLevelBestSpeed:
		return fasthttp.CompressBestSpeed
	case gzipCompressionLevelBestCompression:
		return fasthttp.CompressBestCompression
	case gzipCompressionLevelHuffmanOnly:
		return fasthttp.CompressHuffmanOnly
	default:
		return fasthttp.CompressDefaultCompression
	}
}

type Plugin struct {
	config *Config

	client     *fasthttp.Client
	endpoint   *fasthttp.URI
	authHeader string

	logger     *zap.SugaredLogger
	controller pipeline.OutputPluginController

	batcher      *pipeline.RetriableBatcher
	avgEventSize int

	cancel context.CancelFunc

	// plugin metrics
	sendErrorMetric *prometheus.CounterVec
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > A full URI address of splunk HEC endpoint. Format: `http://127.0.0.1:8088/services/collector`.
	Endpoint string `json:"endpoint" required:"true"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use gzip encoding.
	UseGzip bool `json:"use_gzip" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Gzip compression level. Used if `use_gzip=true`.
	GzipCompressionLevel  string `json:"gzip_compression_level" default:"default" options:"default|no|best-speed|best-compression|huffman-only"` // *
	GzipCompressionLevel_ gzipCompressionLevel

	// > @3@4@5@6
	// >
	// > Token for an authentication for a HEC endpoint.
	Token string `json:"token" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Keep-alive config.
	// >
	// > `KeepAliveConfig` params:
	// > * `max_idle_conn_duration` - idle keep-alive connections are closed after this duration.
	// > By default idle connections are closed after `10s`.
	// > * `max_conn_duration` - keep-alive connections are closed after this duration.
	// > If set to `0` - connection duration is unlimited.
	// > By default connection duration is unlimited.
	KeepAlive KeepAliveConfig `json:"keep_alive" child:"true"` // *

	// > @3@4@5@6
	// >
	// > How many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > Client timeout when sends requests to HTTP Event Collector.
	RequestTimeout  cfg.Duration `json:"request_timeout" default:"1s" parse:"duration"` // *
	RequestTimeout_ time.Duration

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
	// > After this timeout the batch will be sent even if batch isn't completed.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

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
	// > Retention milliseconds for retry to DB.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *
}

type KeepAliveConfig struct {
	// Idle keep-alive connections are closed after this duration.
	MaxIdleConnDuration  cfg.Duration `json:"max_idle_conn_duration" parse:"duration" default:"10s"`
	MaxIdleConnDuration_ time.Duration

	// Keep-alive connections are closed after this duration.
	MaxConnDuration  cfg.Duration `json:"max_conn_duration" parse:"duration" default:"0"`
	MaxConnDuration_ time.Duration
}

type data struct {
	outBuf []byte
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
	p.logger = params.Logger
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)
	p.prepareClient()

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		MaintenanceFn:  p.maintenance,
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

		p.logger.Desugar().Log(level, "can't send data to splunk", zap.Error(err),
			zap.Int("retries", p.config.Retry))
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

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounterVec(
		"output_splunk_send_error",
		"Total splunk send errors",
		"status_code",
	)
}

func (p *Plugin) prepareClient() {
	p.client = &fasthttp.Client{
		ReadTimeout:  p.config.RequestTimeout_,
		WriteTimeout: p.config.RequestTimeout_,

		MaxIdleConnDuration: p.config.KeepAlive.MaxIdleConnDuration_,
		MaxConnDuration:     p.config.KeepAlive.MaxConnDuration_,

		TLSConfig: &tls.Config{
			// TODO: make this configuration option and false by default
			InsecureSkipVerify: true,
		},
	}

	p.endpoint = &fasthttp.URI{}
	if err := p.endpoint.Parse(nil, []byte(p.config.Endpoint)); err != nil {
		p.logger.Fatalf("can't parse splunk endpoint %s: %s", p.config.Endpoint, err.Error())
	}

	p.authHeader = "Splunk " + p.config.Token
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

	root := insaneJSON.Spawn()
	outBuf := data.outBuf[:0]

	batch.ForEach(func(event *pipeline.Event) {
		root.AddField("event").MutateToNode(event.Root.Node)
		outBuf = root.Encode(outBuf)
		_ = root.DecodeString("{}")
	})
	insaneJSON.Release(root)
	data.outBuf = outBuf

	p.logger.Debugf("trying to send: %s", outBuf)

	code, err := p.send(outBuf)
	if err != nil {
		p.sendErrorMetric.WithLabelValues(strconv.Itoa(code)).Inc()
		p.logger.Errorf("can't send data to splunk address=%s: %s", p.config.Endpoint, err.Error())

		// skip retries for bad request
		if code == http.StatusBadRequest {
			return nil
		}
	} else {
		p.logger.Debugf("successfully sent: %s", outBuf)
	}

	return err
}

func (p *Plugin) maintenance(_ *pipeline.WorkerData) {}

func (p *Plugin) send(data []byte) (int, error) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	p.prepareRequest(req, data)

	if err := p.client.DoTimeout(req, resp, p.config.RequestTimeout_); err != nil {
		return 0, fmt.Errorf("can't send request: %w", err)
	}

	respBody := resp.Body()

	var statusCode int
	if statusCode = resp.Header.StatusCode(); statusCode != http.StatusOK {
		return statusCode, fmt.Errorf("bad response: code=%s, body=%s", resp.Header.StatusMessage(), respBody)
	}

	return statusCode, parseSplunkError(respBody)
}

func (p *Plugin) prepareRequest(req *fasthttp.Request, body []byte) {
	req.SetURI(p.endpoint)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set(fasthttp.HeaderAuthorization, p.authHeader)

	if p.config.UseGzip {
		if _, err := fasthttp.WriteGzipLevel(req.BodyWriter(), body, p.config.GzipCompressionLevel_.toFastHTTP()); err != nil {
			req.SetBodyRaw(body)
		} else {
			req.Header.SetContentEncoding(gzipContentEncoding)
		}
	} else {
		req.SetBodyRaw(body)
	}
}

func parseSplunkError(data []byte) error {
	root, err := insaneJSON.DecodeBytes(data)
	defer insaneJSON.Release(root)
	if err != nil {
		return fmt.Errorf("can't decode response: %w", err)
	}

	code := root.Dig("code")
	if code == nil {
		return fmt.Errorf("invalid response format, expecting json with 'code' field, got: %s", data)
	}

	if code.AsInt() > 0 {
		return fmt.Errorf("error while sending to splunk: %s", data)
	}

	return nil
}
