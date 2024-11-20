package elasticsearch

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"strconv"
	"sync"
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
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.
}*/

const (
	outPluginType = "elasticsearch"

	NDJSONContentType = "application/x-ndjson"
)

type Plugin struct {
	config *Config

	client *xhttp.Client

	logger     *zap.Logger
	controller pipeline.OutputPluginController

	batcher      *pipeline.RetriableBatcher
	avgEventSize int

	time         string
	headerPrefix string
	cancel       context.CancelFunc
	mu           *sync.Mutex

	// plugin metrics
	sendErrorMetric      *prometheus.CounterVec
	indexingErrorsMetric prometheus.Counter
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of elasticsearch endpoints in the following format: `SCHEMA://HOST:PORT`
	Endpoints []string `json:"endpoints"  required:"true"` // *

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
	// > It defines the pattern of elasticsearch index name. Use `%` character as a placeholder. Use `index_values` to define values for the replacement.
	// > E.g. if `index_format="my-index-%-%"` and `index_values="service,@@time"` and event is `{"service"="my-service"}`
	// > then index for that event will be `my-index-my-service-2020-01-05`. First `%` replaced with `service` field of the event and the second
	// > replaced with current time(see `time_format` option)
	IndexFormat string `json:"index_format" default:"file-d-%"` // *

	// > @3@4@5@6
	// >
	// > A comma-separated list of event fields which will be used for replacement `index_format`.
	// > There is a special field `@@time` which equals the current time. Use the `time_format` to define a time format.
	// > E.g. `[service, @@time]`
	IndexValues []string `json:"index_values" default:"[@time]" slice:"true"` // *

	// > @3@4@5@6
	// >
	// > The time format pattern to use as value for the `@@time` placeholder.
	// > > Check out [func Parse doc](https://golang.org/pkg/time/#Parse) for details.
	TimeFormat string `json:"time_format" default:"2006-01-02"` // *

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
	// > After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Operation type to be used in batch requests. It can be `index` or `create`. Default is `index`.
	// > > Check out [_bulk API doc](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) for details.
	BatchOpType string `json:"batch_op_type" default:"index" options:"index|create"` // *

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
	p.headerPrefix = `{"` + p.config.BatchOpType + `":{"_index":"`

	if len(p.config.IndexValues) == 0 {
		p.config.IndexValues = append(p.config.IndexValues, "@time")
	}

	p.prepareClient()

	p.maintenance(nil)

	p.logger.Info("starting batcher", zap.Duration("timeout", p.config.BatchFlushTimeout_))

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:        params.PipelineName,
		OutputType:          outPluginType,
		MaintenanceFn:       p.maintenance,
		Controller:          p.controller,
		Workers:             p.config.WorkersCount_,
		BatchSizeCount:      p.config.BatchSize_,
		BatchSizeBytes:      p.config.BatchSizeBytes_,
		FlushTimeout:        p.config.BatchFlushTimeout_,
		MaintenanceInterval: time.Minute,
		MetricCtl:           params.MetricCtl,
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

		p.logger.Log(level, "can't send to the elastic", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)
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
	p.sendErrorMetric = ctl.RegisterCounterVec("output_elasticsearch_send_error", "Total elasticsearch send errors", "status_code")
	p.indexingErrorsMetric = ctl.RegisterCounter("output_elasticsearch_index_error", "Number of elasticsearch indexing errors")
}

func (p *Plugin) prepareClient() {
	config := &xhttp.ClientConfig{
		Endpoints:         prepareEndpoints(p.config.Endpoints),
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

func prepareEndpoints(endpoints []string) []string {
	res := make([]string, 0, len(endpoints))
	for _, e := range endpoints {
		if e[len(e)-1] == '/' {
			e = e[:len(e)-1]
		}
		res = append(res, e+"/_bulk?_source=false")
	}
	return res
}

func (p *Plugin) maintenance(_ *pipeline.WorkerData) {
	p.mu.Lock()
	p.time = time.Now().Format(p.config.TimeFormat)
	p.mu.Unlock()
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

	eventsCount := 0
	begin := make([]int, 0, p.config.BatchSize_+1)
	data.outBuf = data.outBuf[:0]
	batch.ForEach(func(event *pipeline.Event) {
		eventsCount++
		begin = append(begin, len(data.outBuf))
		data.outBuf = p.appendEvent(data.outBuf, event)
	})
	begin = append(begin, len(data.outBuf))

	var f func(left, right int) error
	f = func(left, right int) error {
		if left == right {
			return nil
		}

		statusCode, err := p.client.DoTimeout(
			http.MethodPost,
			NDJSONContentType,
			data.outBuf[begin[left]:begin[right]],
			p.config.ConnectionTimeout_, p.reportESErrors)

		if err != nil {
			p.sendErrorMetric.WithLabelValues(strconv.Itoa(statusCode)).Inc()
			switch statusCode {
			case http.StatusRequestEntityTooLarge:
				// can't send even one log
				if right-left == 1 {
					return err
				}

				middle := (left + right) / 2
				err = f(left, middle)
				if err != nil {
					return err
				}

				errRight := f(middle, right)
				return errRight
			default:
				return err
			}
		}

		return nil
	}

	statusCode, err := p.client.DoTimeout(http.MethodPost, NDJSONContentType, data.outBuf,
		p.config.ConnectionTimeout_, p.reportESErrors)

	if err != nil {
		p.sendErrorMetric.WithLabelValues(strconv.Itoa(statusCode)).Inc()
		switch statusCode {
		case http.StatusBadRequest:
			const errMsg = "can't send to the elastic, non-retryable error occurred (bad request)"
			fields := []zap.Field{zap.Int("status_code", statusCode), zap.Error(err)}
			if p.config.Strict {
				p.logger.Fatal(errMsg, fields...)
			}
			p.logger.Error(errMsg, fields...)
			return nil
		case http.StatusRequestEntityTooLarge:
			if err := f(0, eventsCount); err != nil {
				const errMsg = "can't send to the elastic, non-retryable error occurred (entity too large)"
				fields := []zap.Field{zap.Int("status_code", statusCode), zap.Error(err)}
				if p.config.Strict {
					p.logger.Fatal(errMsg, fields...)
				}
				p.logger.Error(errMsg, fields...)
			}
			return nil
		default:
			p.logger.Error("can't send to the elastic, will try other endpoint", zap.Error(err))
			return err
		}
	}

	return nil
}

func (p *Plugin) appendEvent(outBuf []byte, event *pipeline.Event) []byte {
	// index command
	outBuf = p.appendIndexName(outBuf, event)
	outBuf = append(outBuf, '\n')

	// document
	outBuf, _ = event.Encode(outBuf)
	outBuf = append(outBuf, '\n')

	return outBuf
}

func (p *Plugin) appendIndexName(outBuf []byte, event *pipeline.Event) []byte {
	outBuf = append(outBuf, p.headerPrefix...)
	replacements := 0
	for _, c := range pipeline.StringToByteUnsafe(p.config.IndexFormat) {
		if c != '%' {
			outBuf = append(outBuf, c)
			continue
		}

		if replacements >= len(p.config.IndexValues) {
			p.logger.Fatal("count of placeholders and values isn't match, check index_format/index_values config params")
		}
		value := p.config.IndexValues[replacements]
		replacements++

		if value == "@time" {
			outBuf = append(outBuf, p.time...)
		} else {
			value := event.Root.Dig(value).AsString()
			if value == "" {
				value = "not_set"
			}
			outBuf = append(outBuf, value...)
		}
	}
	outBuf = append(outBuf, "\"}}"...)
	return outBuf
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

// example of an ElasticSearch response that returned an indexing error for the first log:
//
//	{
//	 "took": 5,
//	 "errors": true,
//	 "items": [
//	   {
//	     "index": {
//	       "_index": "logs",
//	       "_type": "_doc",
//	       "_id": "x8YzWowBaqwP8avfpXh8",
//	       "status": 400,
//	       "error": {
//	         "type": "mapper_parsing_exception",
//	         "reason": "failed to parse field [hello] of type [text] in document with id 'x8YzWowBaqwP8avfpXh8'. Preview of field's value: '{test=test}'",
//	         "caused_by": {
//	           "type": "illegal_state_exception",
//	           "reason": "Can't get text on a START_OBJECT at 1:11"
//	         }
//	       }
//	     }
//	   },
//	   {
//	     "index": {
//	       "_index": "logs",
//	       "_type": "_doc",
//	       "_id": "yMYzWowBaqwP8avfpXh8",
//	       "_version": 1,
//	       "result": "created",
//	       "_shards": {
//	         "total": 2,
//	         "successful": 1,
//	         "failed": 0
//	       },
//	       "_seq_no": 4,
//	       "_primary_term": 1,
//	       "status": 201
//	     }
//	   }
//	 ]
//	}
func (p *Plugin) reportESErrors(data []byte) error {
	root, err := insaneJSON.DecodeBytes(data)
	defer insaneJSON.Release(root)
	if err != nil {
		return fmt.Errorf("can't decode response: %w", err)
	}

	if !root.Dig("errors").AsBool() {
		return nil
	}

	items := root.Dig("items").AsArray()
	if len(items) == 0 {
		p.logger.Error("unknown elasticsearch error, 'items' field in the response is empty",
			zap.String("response", root.EncodeToString()),
		)
		return nil
	}

	indexingErrors := 0
	for _, node := range items {
		indexNode := node.Dig("index")
		if indexNode == nil {
			p.logger.Error("unknown elasticsearch response, 'index' field in the response is empty",
				zap.String("response", node.EncodeToString()),
			)
			continue
		}

		if errNode := indexNode.Dig("error"); errNode != nil {
			indexingErrors++
			p.logger.Error("elasticsearch indexing error",
				zap.String("response", errNode.EncodeToString()))
			continue
		}

		if statusCode := indexNode.Dig("status"); statusCode.AsInt() < http.StatusBadRequest {
			continue
		}

		p.logger.Error("unknown elasticsearch error", zap.String("response", node.EncodeToString()))
	}

	if indexingErrors != 0 {
		p.indexingErrorsMetric.Add(float64(indexingErrors))
	}

	p.logger.Error("some events from batch aren't written, check previous logs for more information")
	return nil
}
