package elasticsearch

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/logger"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
	endpoints          []string
	Endpoints          string            `json:"endpoints"`
	IndexFormat        string            `json:"index_format"` // "index-for-service-%-and-time-%"
	IndexValues        []string          `json:"index_values"` // ["service", "@time"]
	TimeFormat         string            `json:"time_format"`
	FlushTimeout       pipeline.Duration `json:"flush_timeout"`
	ConnectionTimeout  pipeline.Duration `json:"connection_timeout"`
	WorkersCount       int               `json:"workers_count"`
	BatchSize          int               `json:"batch_size"`
	IndexErrorWarnOnly bool              `json:"index_error_warn_only"`
}

type Plugin struct {
	client     *http.Client
	config     *Config
	avgLogSize int
	time       string
	batcher    *pipeline.Batcher
	controller pipeline.OutputPluginController
	mu         *sync.Mutex
}

type data struct {
	outBuf []byte
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "elasticsearch",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.avgLogSize = params.PipelineSettings.AvgLogSize
	p.config = config.(*Config)
	p.mu = &sync.Mutex{}

	if p.config.Endpoints == "" {
		logger.Fatalf("endpoints aren't set for elasticsearch output")
	}

	p.config.endpoints = strings.Split(p.config.Endpoints, ",")
	for i, endpoint := range p.config.endpoints {
		if endpoint[len(endpoint)-1] == '/' {
			endpoint = endpoint[:len(endpoint)-1]
		}
		p.config.endpoints[i] = endpoint + "/_bulk?_source=false"
	}

	if p.config.WorkersCount == 0 {
		p.config.WorkersCount = runtime.GOMAXPROCS(0) * 4
	}

	if p.config.FlushTimeout.Duration == 0 {
		p.config.FlushTimeout.Duration = pipeline.DefaultFlushTimeout
	}

	if p.config.ConnectionTimeout.Duration == 0 {
		p.config.ConnectionTimeout.Duration = pipeline.DefaultConnectionTimeout
	}

	if p.config.TimeFormat == "" {
		p.config.TimeFormat = "2006-01-02"
	}

	p.client = &http.Client{
		Timeout: p.config.ConnectionTimeout.Duration,
	}

	if p.config.BatchSize == 0 {
		p.config.BatchSize = params.PipelineSettings.Capacity / 4
	}

	p.maintenance(nil)

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"elasticsearch",
		p.out,
		p.maintenance,
		p.controller,
		p.config.WorkersCount,
		p.config.BatchSize,
		p.config.FlushTimeout.Duration,
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
			outBuf: make([]byte, 0, p.config.BatchSize*p.avgLogSize),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize*p.avgLogSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize*p.avgLogSize)
	}

	data.outBuf = data.outBuf[:0]
	for _, event := range batch.Events {
		data.outBuf = p.appendEvent(data.outBuf, event)
	}

	for {
		endpoint := p.config.endpoints[rand.Int()%len(p.config.endpoints)]
		resp, err := p.client.Post(endpoint, "application/x-ndjson", bytes.NewBuffer(data.outBuf))
		if err != nil {
			logger.Errorf("can't send batch to %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		respContent, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Errorf("can't read batch response from %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		root, err := insaneJSON.DecodeBytes(respContent)
		if err != nil {
			logger.Errorf("wrong response from %s, will try other endpoint: %s", endpoint, err.Error())
			continue
		}

		if root.Dig("errors").AsBool() {
			for _, node := range root.Dig("items").AsArray() {
				errNode := node.Dig("index", "error")
				if errNode != nil {
					logger.Warnf("indexing error: %s", errNode.Dig("reason").AsString())
				}
			}

			if !p.config.IndexErrorWarnOnly {
				logger.Fatalf("batch send error")
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

		if replacements >= len(p.config.IndexValues) {
			logger.Fatalf("wrong index format / values")
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

func (p *Plugin) maintenance(workerData *pipeline.WorkerData) {
	p.mu.Lock()
	p.time = time.Now().Format(p.config.TimeFormat)
	p.mu.Unlock()
}
