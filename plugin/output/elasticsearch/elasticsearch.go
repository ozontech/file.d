package elasticsearch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
)

/*{ introduction
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.
}*/

const (
	outPluginType = "elasticsearch"
	subsystemName = "output_elasticsearch"

	// errors
	sendErrorCounter = "send_error"
	indexingErrors   = "index_error"

	NDJSONContentType = "application/x-ndjson"
)

type Plugin struct {
	logger       *zap.SugaredLogger
	client       *fasthttp.Client
	cancel       context.CancelFunc
	config       *Config
	authHeader   []byte
	avgEventSize int
	time         string
	batcher      *pipeline.Batcher
	controller   pipeline.OutputPluginController
	mu           *sync.Mutex
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The list of elasticsearch endpoints in the following format: `SCHEMA://HOST:PORT`
	Endpoints []string `json:"endpoints"  required:"true"` //*

	//> @3@4@5@6
	//>
	//> Elastic username for Basic authentication (username and password).
	Username string `json:"username"` //*

	//> @3@4@5@6
	//>
	//> Elastic password for Basic authentication (username and password).
	Password string `json:"password"` //*

	//> @3@4@5@6
	//>
	//> Elastic secret token for API key authentication.
	APIKey string `json:"api_key"` //*

	//> @3@4@5@6
	//>
	//> Custom certificate authority used to sign the certificates of cluster nodes.
	CACert string `json:"ca_cert"` //*

	//> @3@4@5@6
	//>
	//> Custom certificate authority used to sign the certificates of cluster nodes.
	CertificateFingerprint string `json:"certificate_fingerprint"` //*

	//> @3@4@5@6
	//>
	//> It defines the pattern of elasticsearch index name. Use `%` character as a placeholder. Use `index_values` to define values for the replacement.
	//> E.g. if `index_format="my-index-%-%"` and `index_values="service,@@time"` and event is `{"service"="my-service"}`
	//> then index for that event will be `my-index-my-service-2020-01-05`. First `%` replaced with `service` field of the event and the second
	//> replaced with current time(see `time_format` option)
	IndexFormat string `json:"index_format" default:"file-d-%"` //*

	//> @3@4@5@6
	//>
	//> A comma-separated list of event fields which will be used for replacement `index_format`.
	//> There is a special field `@@time` which equals the current time. Use the `time_format` to define a time format.
	//> E.g. `[service, @@time]`
	IndexValues []string `json:"index_values" default:"[@time]" slice:"true"` //*

	//> @3@4@5@6
	//>
	//> The time format pattern to use as value for the `@@time` placeholder.
	//> > Check out [func Parse doc](https://golang.org/pkg/time/#Parse) for details.
	TimeFormat string `json:"time_format" default:"2006-01-02"` //*

	//> @3@4@5@6
	//>
	//> It defines how much time to wait for the connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s" parse:"duration"` //*
	ConnectionTimeout_ time.Duration

	//> @3@4@5@6
	//>
	//> It defines how many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3@4@5@6
	//>
	//> A maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4"  parse:"expression"` //*
	BatchSize_ int

	//> @3@4@5@6
	//>
	//> After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` //*
	BatchFlushTimeout_ time.Duration
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
	p.mu = &sync.Mutex{}

	if len(p.config.IndexValues) == 0 {
		p.config.IndexValues = append(p.config.IndexValues, "@time")
	}

	for i, endpoint := range p.config.Endpoints {
		if endpoint[len(endpoint)-1] == '/' {
			endpoint = endpoint[:len(endpoint)-1]
		}
		p.config.Endpoints[i] = endpoint + "/_bulk?_source=false"
	}

	p.client = &fasthttp.Client{}
	var (
		err    error
		caCert []byte
	)
	if p.config.CACert != "" {
		caCert, err = os.ReadFile(p.config.CACert)
		if err != nil {
			p.logger.Fatalf("can't read CACert file: %s", err.Error())
		}

		certPool := x509.NewCertPool()
		certPool.AppendCertsFromPEM(caCert)
		p.client.TLSConfig = &tls.Config{
			RootCAs: certPool,
		}
	}

	p.authHeader = p.getAuthHeader()

	p.maintenance(nil)

	p.registerPluginMetrics()

	p.logger.Infof("starting batcher: timeout=%d", p.config.BatchFlushTimeout_)
	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		outPluginType,
		p.out,
		p.maintenance,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		time.Minute,
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

func (p *Plugin) registerPluginMetrics() {
	stats.RegisterCounter(&stats.MetricDesc{
		Name:      sendErrorCounter,
		Subsystem: subsystemName,
		Help:      "Total elasticsearch send errors",
	})

	stats.RegisterCounter(&stats.MetricDesc{
		Name:      indexingErrors,
		Subsystem: subsystemName,
		Help:      "Number of elasticsearch indexing errors",
	})
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
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
	for _, event := range batch.Events {
		data.outBuf = p.appendEvent(data.outBuf, event)
	}

	for {
		if err := p.send(data.outBuf); err != nil {
			stats.GetCounter(subsystemName, sendErrorCounter).Inc()
			p.logger.Errorf("can't send to the elastic, will try other endpoint: %s", err.Error())
		} else {
			break
		}
	}
}

func (p *Plugin) send(body []byte) error {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	endpoint := p.config.Endpoints[rand.Int()%len(p.config.Endpoints)]
	req.SetRequestURI(endpoint)
	req.SetBodyRaw(body)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.SetContentType(NDJSONContentType)
	p.setAuthHeader(req)

	if err := p.client.DoTimeout(req, resp, p.config.ConnectionTimeout_); err != nil {
		time.Sleep(time.Second)
		return fmt.Errorf("can't send batch to %s: %s", endpoint, err.Error())
	}

	respContent := resp.Body()

	if statusCode := resp.Header.StatusCode(); statusCode < http.StatusOK || statusCode > http.StatusAccepted {
		return fmt.Errorf("response status from %s isn't OK: status=%d, body=%s", endpoint, statusCode, string(respContent))
	}

	root, err := insaneJSON.DecodeBytes(respContent)
	if err != nil {
		return fmt.Errorf("wrong response from %s: %s", endpoint, err.Error())
	}
	defer insaneJSON.Release(root)

	if root.Dig("errors").AsBool() {
		errors := 0
		for _, node := range root.Dig("items").AsArray() {
			errNode := node.Dig("index", "error")
			if errNode != nil {
				errors += 1
				p.logger.Errorf("indexing error: %s", errNode.EncodeToString())
			}
		}

		if errors != 0 {
			stats.GetCounter(subsystemName, indexingErrors).Add(float64(errors))
		}

		p.controller.Error("some events from batch isn't written")
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
	outBuf = append(outBuf, `{"index":{"_index":"`...)
	replacements := 0
	for _, c := range pipeline.StringToByteUnsafe(p.config.IndexFormat) {
		if c != '%' {
			outBuf = append(outBuf, c)
			continue
		}

		if replacements >= len(p.config.IndexValues) {
			p.logger.Fatalf("count of placeholders and values isn't match, check index_format/index_values config params")
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
		return []byte("Bearer " + p.config.APIKey)
	} else if p.config.Username != "" && p.config.Password != "" {
		credentials := []byte(p.config.Username + ":" + p.config.Password)
		buf := make([]byte, base64.StdEncoding.EncodedLen(len(credentials)))
		base64.StdEncoding.Encode(buf, credentials)
		return append([]byte("Basic "), buf...)
	}
	return nil
}

func (p *Plugin) setAuthHeader(req *fasthttp.Request) {
	strAuthorization := []byte(fasthttp.HeaderAuthorization)
	if p.authHeader != nil {
		req.Header.SetBytesKV(strAuthorization, p.authHeader)
	}
}
