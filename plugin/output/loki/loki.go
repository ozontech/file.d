package loki

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"

	"github.com/prometheus/client_golang/prometheus"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends the logs batches to Loki using HTTP API.
}*/

const (
	outPluginType = "loki"
)

type data struct {
	outBuf []byte
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > TCP Loki host
	// >
	// > Example host
	// >
	// > 127.0.0.1 or localhost
	Host string `json:"host" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Loki port
	// >
	// > Example port
	// >
	// > 3100
	Port string `json:"port" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, if true set OrgID
	AuthEnabled bool `json:"auth_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, if set true set OrgID
	// >
	// > Example organization id
	// >
	// > example-org
	OrgID string `json:"org_id"` // *

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
	// > Path or content of a PEM-encoded client certificate file.
	ClientCert string `json:"client_cert"` // *

	// > @3@4@5@6
	// >
	// > > Path or content of a PEM-encoded client key file.
	ClientKey string `json:"client_key"` // *

	// > @3@4@5@6
	// >
	// > Path or content of a PEM-encoded CA file. This can be a path or the content of the certificate.
	CACert string `json:"ca_cert"` // *

	// > @3@4@5@6
	// >
	// > Client timeout when sends requests to Loki HTTP API.
	RequestTimeout  cfg.Duration `json:"request_timeout" default:"1s" parse:"duration"` // *
	RequestTimeout_ time.Duration

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

type Plugin struct {
	controller   pipeline.OutputPluginController
	logger       *zap.SugaredLogger
	config       *Config
	avgEventSize int

	httpClient *http.Client
	batcher    *pipeline.RetriableBatcher

	// plugin metrics
	sendErrorMetric prometheus.Counter
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
	p.logger = params.Logger
	p.avgEventSize = params.PipelineSettings.AvgEventSize

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

		p.logger.Desugar().Log(level, "can't send data to loki", zap.Error(err),
			zap.Int("retries", p.config.Retry))
	}

	p.httpClient = p.newClient(p.config.RequestTimeout_)
	p.batcher = pipeline.NewRetriableBatcher(
		batcherOpts,
		p.out,
		backoffOpts,
		onError,
	)
}

func (p *Plugin) Stop() {
	p.httpClient.CloseIdleConnections()
	p.batcher.Stop()
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

	code, err := p.send(context.Background(), outBuf)
	if err != nil {
		p.sendErrorMetric.Inc()
		p.logger.Errorf("can't send data to Loki address=%s: %v", fmt.Sprintf("%s:%s", p.config.Host, p.config.Port), err.Error())

		// skip retries for bad request
		if code == http.StatusBadRequest {
			return nil
		}
	} else {
		p.logger.Debugf("successfully sent: %s", outBuf)
	}

	return err
}

func (p *Plugin) send(ctx context.Context, data []byte) (int, error) {
	r := bytes.NewReader(data)

	url := fmt.Sprintf("http://%s:%s/loki/api/v1/push", p.config.Host, p.config.Port)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return 0, fmt.Errorf("can't create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if p.config.AuthEnabled {
		req.Header.Set("X-Scope-OrgID", p.config.OrgID)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("can't send request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("can't read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("bad response: code=%s, body=%s", resp.Status, b)
	}

	root, err := insaneJSON.DecodeBytes(b)
	defer insaneJSON.Release(root)
	if err != nil {
		return resp.StatusCode, fmt.Errorf("can't decode response: %w", err)
	}

	code := root.Dig("code")
	if code == nil {
		return resp.StatusCode, fmt.Errorf("invalid response format, expecting json with 'code' field, got: %s", string(b))
	}

	if code.AsInt() > 0 {
		return resp.StatusCode, fmt.Errorf("error while sending to splunk: %s", string(b))
	}

	return resp.StatusCode, nil
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_loki_send_error", "Total Loki send errors")
}

func (p *Plugin) newClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport)

	if p.config.TLSEnabled {
		transport.TLSClientConfig = &tls.Config{
			InsecureSkipVerify: p.config.TLSSkipVerify,
		}
	}

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}

	return client
}
