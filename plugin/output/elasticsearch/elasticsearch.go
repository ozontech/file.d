package elasticsearch

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtls"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/valyala/fasthttp"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.
}*/

const (
	outPluginType     = "elasticsearch"
	NDJSONContentType = "application/x-ndjson"
)

var (
	strAuthorization = []byte(fasthttp.HeaderAuthorization)
)

type Plugin struct {
	logger       *zap.Logger
	client       *fasthttp.Client
	endpoints    []*fasthttp.URI
	cancel       context.CancelFunc
	config       *Config
	authHeader   []byte
	avgEventSize int
	time         string
	headerPrefix string
	batcher      *pipeline.Batcher
	controller   pipeline.OutputPluginController
	mu           *sync.Mutex

	// plugin metrics
	sendErrorMetric      prometheus.Counter
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
	// > Multiplier for exponentially increase retention beetween retries
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
	p.logger = params.Logger.Desugar()
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)
	p.mu = &sync.Mutex{}
	p.headerPrefix = `{"` + p.config.BatchOpType + `":{"_index":"`

	if len(p.config.IndexValues) == 0 {
		p.config.IndexValues = append(p.config.IndexValues, "@time")
	}

	for _, endpoint := range p.config.Endpoints {
		if endpoint[len(endpoint)-1] == '/' {
			endpoint = endpoint[:len(endpoint)-1]
		}

		uri := &fasthttp.URI{}
		if err := uri.Parse(nil, []byte(endpoint+"/_bulk?_source=false")); err != nil {
			logger.Fatalf("can't parse ES endpoint %s: %s", endpoint, err.Error())
		}

		p.endpoints = append(p.endpoints, uri)
	}

	p.client = &fasthttp.Client{
		ReadTimeout:     p.config.ConnectionTimeout_ * 2,
		WriteTimeout:    p.config.ConnectionTimeout_ * 2,
		MaxConnDuration: time.Minute * 5,
	}

	if p.config.CACert != "" {
		b := xtls.NewConfigBuilder()
		err := b.AppendCARoot(p.config.CACert)
		if err != nil {
			p.logger.Fatal("can't append CA root", zap.Error(err))
		}

		p.client.TLSConfig = b.Build()
	}

	p.authHeader = p.getAuthHeader()

	p.maintenance(nil)

	p.logger.Info("starting batcher", zap.Duration("timeout", p.config.BatchFlushTimeout_))
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
		MaintenanceInterval:              time.Minute,
		MetricCtl:                        params.MetricCtl,
		Retry:                            p.config.Retry,
		RetryRetention:                   p.config.Retention_,
		RetryRetentionExponentMultiplier: p.config.RetentionExponentMultiplier,
	})

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
	p.sendErrorMetric = ctl.RegisterCounter("output_elasticsearch_send_error", "Total elasticsearch send errors")
	p.indexingErrorsMetric = ctl.RegisterCounter("output_elasticsearch_index_error", "Number of elasticsearch indexing errors")
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

	data.outBuf = data.outBuf[:0]
	batch.ForEach(func(event *pipeline.Event) {
		data.outBuf = p.appendEvent(data.outBuf, event)
	})

	err := backoff.Retry(func() error {
		err := p.send(data.outBuf)
		if err != nil {
			p.sendErrorMetric.Inc()
			p.logger.Error("can't send to the elastic, will try other endpoint", zap.Error(err))
		}
		return err
	}, *workerBackoff)

	if err != nil {
		var errLogFunc func(args ...interface{})
		if p.config.FatalOnFailedInsert {
			errLogFunc = p.logger.Sugar().Fatal
		} else {
			errLogFunc = p.logger.Sugar().Error
		}

		errLogFunc("can't send to the elastic", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)
	}
}

func (p *Plugin) send(body []byte) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	endpoint := p.endpoints[rand.Int()%len(p.endpoints)]
	req.SetURI(endpoint)
	req.SetBodyRaw(body)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType(NDJSONContentType)
	p.setAuthHeader(req)

	if err := p.client.DoTimeout(req, resp, p.config.ConnectionTimeout_); err != nil {
		return fmt.Errorf("can't send batch to %s: %s", endpoint.String(), err.Error())
	}

	respContent := resp.Body()

	if statusCode := resp.Header.StatusCode(); statusCode < http.StatusOK || statusCode > http.StatusAccepted {
		return fmt.Errorf("response status from %s isn't OK: status=%d, body=%s", endpoint.String(), statusCode, string(respContent))
	}

	root, err := insaneJSON.DecodeBytes(respContent)
	if err != nil {
		return fmt.Errorf("wrong response from %s: %s", endpoint.String(), err.Error())
	}
	defer insaneJSON.Release(root)

	p.reportESErrors(root)

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

func (p *Plugin) maintenance(_ *pipeline.WorkerData) {
	p.mu.Lock()
	p.time = time.Now().Format(p.config.TimeFormat)
	p.mu.Unlock()
}

func (p *Plugin) getAuthHeader() []byte {
	if p.config.APIKey != "" {
		return []byte("ApiKey " + p.config.APIKey)
	}
	if p.config.Username != "" && p.config.Password != "" {
		credentials := []byte(p.config.Username + ":" + p.config.Password)
		buf := make([]byte, base64.StdEncoding.EncodedLen(len(credentials)))
		base64.StdEncoding.Encode(buf, credentials)
		return append([]byte("Basic "), buf...)
	}
	return nil
}

func (p *Plugin) setAuthHeader(req *fasthttp.Request) {
	if p.authHeader != nil {
		req.Header.SetBytesKV(strAuthorization, p.authHeader)
	}
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
func (p *Plugin) reportESErrors(root *insaneJSON.Root) {
	if !root.Dig("errors").AsBool() {
		return
	}

	items := root.Dig("items").AsArray()
	if len(items) == 0 {
		p.logger.Error("unknown elasticsearch error, 'items' field in the response is empty",
			zap.String("response", root.EncodeToString()),
		)
		return
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
}
