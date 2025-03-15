package loki

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

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
	Labels []Label `json:"labels"`

	// > @3@4@5@6
	// >
	// > Message field from log to be mapped to loki
	// >
	// > Example
	// >
	// > message
	MessageField string `json:"message_field" required:"true"`

	// > @3@4@5@6
	// >
	// > Timestamp field from log to be mapped to loki
	// >
	// > Example
	// >
	// > timestamp
	TimestampField string `json:"timestamp_field" required:"true"`

	// > @3@4@5@6
	// >
	// > Authorization enabled, if true set auth method like tenant, basic auth or bearer
	AuthEnabled bool `json:"auth_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, if set true set TenantID
	// >
	// > Example
	// >
	// > example-org
	TenantID string `json:"tenant_id"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, if auth username set provide auth password
	// >
	// > Example
	// >
	// > username
	AuthUsername string `json:"username"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, provide basic auth password if basic auth username is provided
	// >
	// > Example
	// >
	// > password
	AuthPassword string `json:"password"` // *

	// > @3@4@5@6
	// >
	// > Authorization enabled, provide bearer token if you have Bearer authorization
	// >
	// > Example
	// >
	// > token
	BearerToken string `json:"bearer_token"` // *

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
	p.registerMetrics(params.MetricCtl)

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

	p.batcher.Start(context.Background())
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
	if err := root.DecodeString(`{"data":[]}`); err != nil {
		p.logger.Errorf("failed to decode json: %v", err)
		return err
	}

	outBuf := data.outBuf[:0]

	dataArr := root.Dig("data")

	var jsonEvent string
	batch.ForEach(func(event *pipeline.Event) {
		jsonEvent = event.Root.Node.EncodeToString()
		dataArr.AddElement().MutateToJSON(root, jsonEvent)
	})

	data.outBuf = root.Encode(outBuf)
	insaneJSON.Release(root)

	code, err := p.send(context.Background(), data.outBuf)
	if err != nil {
		p.sendErrorMetric.Inc()
		p.logger.Errorf("can't send data to Loki address=%s: %v", p.config.Address, err.Error())

		// skip retries for bad request or time format errors
		if code == http.StatusBadRequest || errors.Is(err, errUnixNanoFormat) {
			return nil
		}
	} else {
		p.logger.Debugf("successfully sent: %s", outBuf)
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
		p.logger.Errorf("failed to decode json: %v", err)
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
				StreamLabels: p.labels(),
				Values:       values,
			},
		},
	}

	data, err := json.Marshal(output)
	if err != nil {
		return 0, err
	}

	p.logger.Info("sent", string(data))

	r := bytes.NewReader(data)

	url := fmt.Sprintf("%s/loki/api/v1/push", p.config.Address)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, r)
	if err != nil {
		return 0, fmt.Errorf("can't create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if p.config.AuthEnabled {
		p.setAuthenticationHeaders(req)
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

	if resp.StatusCode != http.StatusNoContent {
		return resp.StatusCode, fmt.Errorf("bad response: code=%s, body=%s", resp.Status, b)
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

func (p *Plugin) setAuthenticationHeaders(req *http.Request) {
	if p.config.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", p.config.TenantID)
	}

	if p.config.AuthUsername != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s:%s", p.config.AuthUsername, p.config.AuthPassword))
	}

	if p.config.BearerToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", p.config.BearerToken))
	}
}

func (p *Plugin) labels() map[string]string {
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
