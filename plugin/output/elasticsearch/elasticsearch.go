package elasticsearch

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/ozonru/file.d/cfg"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"

	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.
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

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The list of elasticsearch endpoints in the following format: `SCHEMA://HOST:PORT`
	Endpoints []string `json:"endpoints"  required:"true"` //*

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

	if len(p.config.IndexValues) == 0 {
		p.config.IndexValues = append(p.config.IndexValues, "@time")
	}

	for i, endpoint := range p.config.Endpoints {
		if endpoint[len(endpoint)-1] == '/' {
			endpoint = endpoint[:len(endpoint)-1]
		}
		p.config.Endpoints[i] = endpoint + "/_bulk?_source=false"
	}

	p.client = &http.Client{
		Timeout: p.config.ConnectionTimeout_,
	}

	p.maintenance(nil)

	p.logger.Infof("starting batcher: timeout=%d", p.config.BatchFlushTimeout_)
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
		endpoint := p.config.Endpoints[rand.Int()%len(p.config.Endpoints)]
		resp, err := p.client.Post(endpoint, "application/x-ndjson", bytes.NewBuffer(data.outBuf))
		if err != nil {
			p.logger.Errorf("can't send batch to %s, will try other endpoint: %s", endpoint, err.Error())
			time.Sleep(time.Second)
			continue
		}

		respContent, err := ioutil.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			p.logger.Errorf("can't read response from %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		if resp.StatusCode < http.StatusOK || resp.StatusCode > http.StatusAccepted {
			p.logger.Errorf("response status from %s isn't OK, will try other endpoint: status=%d, body=%s", endpoint, resp.StatusCode, respContent)
			continue
		}

		root, err := insaneJSON.DecodeBytes(respContent)
		if err != nil {
			p.logger.Errorf("wrong response from %s, will try other endpoint: %s", endpoint, err.Error())
			insaneJSON.Release(root)
			continue
		}

		if root.Dig("errors").AsBool() {
			for _, node := range root.Dig("items").AsArray() {
				errNode := node.Dig("index", "error")
				if errNode != nil {
					p.logger.Errorf("indexing error: %s", errNode.EncodeToString())
				}
			}

			p.controller.Error("some events from batch isn't written")
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
