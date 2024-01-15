// Package splunk is an output plugin that sends events to splunk database.
package splunk

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It sends events to splunk.
}*/

const (
	outPluginType = "splunk"
)

type Plugin struct {
	config       *Config
	client       http.Client
	logger       *zap.SugaredLogger
	avgEventSize int
	batcher      *pipeline.Batcher
	controller   pipeline.OutputPluginController

	// plugin metrics
	sendErrorMetric prometheus.Counter
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
	// > Token for an authentication for a HEC endpoint.
	Token string `json:"token" required:"true"` // *

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
	p.client = p.newClient(p.config.RequestTimeout_)

	p.batcher = pipeline.NewBatcher(pipeline.BatcherOptions{
		PipelineName:                     params.PipelineName,
		OutputType:                       outPluginType,
		OutFn:                            p.out,
		MaintenanceFn:                    p.maintenance,
		Controller:                       p.controller,
		Workers:                          p.config.WorkersCount_,
		BatchSizeCount:                   p.config.BatchSize_,
		BatchSizeBytes:                   p.config.BatchSizeBytes_,
		FlushTimeout:                     p.config.BatchFlushTimeout_,
		MetricCtl:                        params.MetricCtl,
		Retry:                            p.config.Retry,
		RetryRetention:                   p.config.Retention_,
		RetryRetentionExponentMultiplier: p.config.RetentionExponentMultiplier,
	})

	p.batcher.Start(context.TODO())
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_splunk_send_error", "Total splunk send errors")
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch, workerBackoff *backoff.BackOff) {
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

	(*workerBackoff).Reset()
	err := backoff.Retry(func() error {
		err := p.send(outBuf)
		if err != nil {
			p.sendErrorMetric.Inc()
			p.logger.Errorf("can't send data to splunk address=%s: %s", p.config.Endpoint, err.Error())
		}
		return err
	}, *workerBackoff)

	if err != nil {
		var errLogFunc func(args ...interface{})
		if p.config.FatalOnFailedInsert {
			errLogFunc = p.logger.Fatal
		} else {
			errLogFunc = p.logger.Error
		}

		errLogFunc("can't send data to splunk", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)
	}

	p.logger.Debugf("successfully sent: %s", outBuf)
}

func (p *Plugin) maintenance(_ *pipeline.WorkerData) {}

func (p *Plugin) newClient(timeout time.Duration) http.Client {
	return http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				// TODO: make this configuration option and false by default
				InsecureSkipVerify: true,
			},
		},
	}
}

func (p *Plugin) send(data []byte) error {
	r := bytes.NewReader(data)
	// todo pass context from parent.
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, p.config.Endpoint, r)
	if err != nil {
		return fmt.Errorf("can't create request: %w", err)
	}

	req.Header.Set("Authorization", "Splunk "+p.config.Token)
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("can't send request: %w", err)
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("can't send request: %s", resp.Status)
	}

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("can't read response: %w", err)
	}

	root, err := insaneJSON.DecodeBytes(b)
	defer insaneJSON.Release(root)
	if err != nil {
		return fmt.Errorf("can't decode response: %w", err)
	}

	code := root.Dig("code")
	if code == nil {
		return fmt.Errorf("invalid response format, expecting json with 'code' field, got: %s", string(b))
	}

	if code.AsInt() > 0 {
		return fmt.Errorf("error while sending to splunk: %s", string(b))
	}

	return nil
}
