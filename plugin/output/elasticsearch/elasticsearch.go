package elasticsearch

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/file-d/cfg"
	"go.uber.org/zap"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin writes events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs batch will be infinitely tries to be delivered to random endpoint.
}*/

type Plugin struct {
	logger     *zap.SugaredLogger
	client     *http.Client
	config     *Config
	avgLogSize int
	time       string
	batcher    *pipeline.Batcher
	controller pipeline.OutputPluginController
	mu         *sync.Mutex
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Comma separated list of elasticsearch endpoints in format `SCHEMA://HOST:PORT`
	Endpoints  string `json:"endpoints" parse:"list" required:"true"` //*
	Endpoints_ []string

	//> @3 @4 @5 @6
	//>
	//> Defines pattern of elasticsearch index name. Use `%` character as a placeholder. Use `index_values` to define values for replacement.
	//> E.g. if `index_format="my-index-%-%"` and `index_values="service,@@time"` and event is `{"service"="my-service"}`
	//> then index for that event will be `my-index-my-service-2020-01-05`. First `%` replaced with `service` field of the event and the second
	//> replaced with current time(see `time_format` option)
	IndexFormat string `json:"index_format" default:"file-d-%"` //*

	//> @3 @4 @5 @6
	//>
	//> Comma-separated list of event fields which will be used for replacement `index_format`.
	//> There is a special field `@@time` which equals to current time. Use `time_format` to define time format.
	//> E.g. `service,@@time`
	IndexValues  string `json:"index_values" default:"@time" parse:"list"` //*
	IndexValues_ []string

	//> @3 @4 @5 @6
	//>
	//> Time format pattern to use as value for the `@@time` placeholder.
	//> > Check out https://golang.org/pkg/time/#Parse for details.
	TimeFormat string `json:"time_format" default:"2006-01-02"` //*

	//> @3 @4 @5 @6
	//>
	//> How much time to wait for connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s"` //*
	ConnectionTimeout_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> How much workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3 @4 @5 @6
	//>
	//> Maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4"  parse:"expression"` //*
	BatchSize_ int

	//> @3 @4 @5 @6
	//>
	//> After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms"` //*
	BatchFlushTimeout_ time.Duration

	//> @3 @4 @5 @6
	//>
	//> If set to `false`, indexing error won't lead to an fatal and exit.
	//> todo: my it be useful for all plugins?
	StrictMode bool `json:"strict_mode" default:"true"` //*
}

type data struct {
	outBuf []byte
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "elasticsearch",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.avgLogSize = params.PipelineSettings.AvgLogSize
	p.config = config.(*Config)
	p.mu = &sync.Mutex{}

	p.config.Endpoints_ = strings.Split(p.config.Endpoints, ",")
	for i, endpoint := range p.config.Endpoints_ {
		if endpoint[len(endpoint)-1] == '/' {
			endpoint = endpoint[:len(endpoint)-1]
		}
		p.config.Endpoints_[i] = endpoint + "/_bulk?_source=false"
	}

	p.client = &http.Client{
		Timeout: p.config.ConnectionTimeout_,
	}

	p.maintenance(nil)

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"elasticsearch",
		p.out,
		p.maintenance,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		time.Minute,
	)
	p.batcher.Start()
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgLogSize),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgLogSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgLogSize)
	}

	data.outBuf = data.outBuf[:0]
	for _, event := range batch.Events {
		data.outBuf = p.appendEvent(data.outBuf, event)
	}

	for {
		endpoint := p.config.Endpoints_[rand.Int()%len(p.config.Endpoints_)]
		resp, err := p.client.Post(endpoint, "application/x-ndjson", bytes.NewBuffer(data.outBuf))
		if err != nil {
			p.logger.Errorf("can't send batch to %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		respContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			p.logger.Errorf("can't read batch response from %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		root, err := insaneJSON.DecodeBytes(respContent)
		if err != nil {
			p.logger.Errorf("wrong response from %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		if root.Dig("errors").AsBool() {
			for _, node := range root.Dig("items").AsArray() {
				errNode := node.Dig("index", "error")
				if errNode != nil {
					p.logger.Warnf("indexing error: %s", errNode.Dig("reason").AsString())
				}
			}

			if p.config.StrictMode {
				p.logger.Fatalf("batch send error")
			}
		}

		insaneJSON.Release(root)
		break
	}
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

		if replacements >= len(p.config.IndexValues_) {
			p.logger.Fatalf("count of placeholders and values isn't match, check index_format/index_values config params")
		}
		value := p.config.IndexValues_[replacements]
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
