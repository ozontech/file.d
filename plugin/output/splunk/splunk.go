// Package splunk is an output plugin that sends events to splunk database.
package splunk

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
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
It sends events to splunk.

By default it only stores original event under the "event" key according to the Splunk output format.

If other fields are required it is possible to copy fields values from the original event to the other
fields relative to the output json. Copies are not allowed directly to the root of output event or
"event" field and any of its subfields.

For example, timestamps and service name can be copied to provide additional meta data to the Splunk:

```yaml
copy_fields:
  - from: ts
  	to: time
  - from: service
  	to: fields.service_name
```

Here the plugin will lookup for "ts" and "service" fields in the original event and if they are present
they will be copied to the output json starting on the same level as the "event" key. If the field is not
found in the original event plugin will not populate new field in output json.

In:

```json
{
  "ts":"1723651045",
  "service":"some-service",
  "message":"something happened"
}
```

Out:

```json
{
  "event": {
    "ts":"1723651045",
    "service":"some-service",
    "message":"something happened"
  },
  "time": "1723651045",
  "fields": {
    "service_name": "some-service"
  }
}
```
}*/

const (
	outPluginType = "splunk"
)

type copyFieldPaths struct {
	fromPath []string
	toPath   []string
}

type Plugin struct {
	config *Config

	client *xhttp.Client

	copyFieldsPaths []copyFieldPaths

	logger     *zap.SugaredLogger
	controller pipeline.OutputPluginController

	batcher      *pipeline.RetriableBatcher
	avgEventSize int

	cancel context.CancelFunc

	// plugin metrics
	sendErrorMetric *prometheus.CounterVec
}

type CopyField struct {
	From string `json:"from"`
	To   string `json:"to"`
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
	GzipCompressionLevel string `json:"gzip_compression_level" default:"default" options:"default|no|best-speed|best-compression|huffman-only"` // *

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

	// > @3@4@5@6
	// >
	// > List of field paths copy `from` field in original event `to` field in output json.
	// > To fields paths are relative to output json - one level higher since original
	// > event is stored under the "event" key. Supports nested fields in both from and to.
	// > Supports copying whole original event, but does not allow to copy directly to the output root
	// > or the "event" key with any of its subkeys.
	CopyFields []CopyField `json:"copy_fields" slice:"true"` // *
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

	for _, cf := range p.config.CopyFields {
		if cf.To == "" {
			p.logger.Error("copies to the root are not allowed")
			continue
		}
		if cf.To == "event" || strings.HasPrefix(cf.To, "event.") {
			p.logger.Error("copies to the `event` field or any of its subfields are not allowed")
			continue
		}
		cf := copyFieldPaths{
			fromPath: cfg.ParseFieldSelector(cf.From),
			toPath:   cfg.ParseFieldSelector(cf.To),
		}
		p.copyFieldsPaths = append(p.copyFieldsPaths, cf)
	}

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

func (p *Plugin) Stop() {
	p.batcher.Stop()
	p.cancel()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounterVec(
		"output_splunk_send_error",
		"Total splunk send errors",
		"status_code",
	)
}

func (p *Plugin) prepareClient() {
	config := &xhttp.ClientConfig{
		Endpoints:         []string{p.config.Endpoint},
		ConnectionTimeout: p.config.RequestTimeout_,
		AuthHeader:        "Splunk " + p.config.Token,
		KeepAlive: &xhttp.ClientKeepAliveConfig{
			MaxConnDuration:     p.config.KeepAlive.MaxConnDuration_,
			MaxIdleConnDuration: p.config.KeepAlive.MaxIdleConnDuration_,
		},
		TLS: &xhttp.ClientTLSConfig{
			// TODO: make this configuration option and false by default
			InsecureSkipVerify: true,
		},
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
		// "event" field is necessary, it always contains full event data
		root.AddField("event").MutateToNode(event.Root.Node)
		// copy data from original event to other fields, like event's "ts" to outbuf's "time"
		for _, cf := range p.copyFieldsPaths {
			fieldVal := event.Root.Dig(cf.fromPath...)
			if fieldVal == nil {
				continue
			}
			pipeline.CreateNestedField(root, cf.toPath).MutateToNode(fieldVal)
		}
		outBuf = root.Encode(outBuf)
		_ = root.DecodeString("{}")
	})
	insaneJSON.Release(root)
	data.outBuf = outBuf

	p.logger.Debugf("trying to send: %s", outBuf)

	code, err := p.client.DoTimeout(http.MethodPost, "", outBuf,
		p.config.RequestTimeout_, parseSplunkError)

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
