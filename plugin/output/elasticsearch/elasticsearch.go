package elasticsearch

import (
	"bytes"
	"fmt"
	"gitlab.ozon.ru/sre/filed/logger"
	"math/rand"
	"net/http"
	"path"
	"runtime"
	"strings"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
	endpoints         []string
	Endpoints         string            `json:"endpoints"`
	IndexFormat       string            `json:"index_format"` // "index-for-service-%-and-time-%"
	IndexValues       []string          `json:"index_values"` // ["service", "@time"]
	TimeField         string            `json:"time_field"`
	TimeFormat        string            `json:"time_format"`
	FlushTimeout      pipeline.Duration `json:"flush_timeout"`
	ConnectionTimeout pipeline.Duration `json:"connection_timeout"`
	WorkersCount      int               `json:"workers_count"`
	BatchSize         int               `json:"batch_size"`
}

type Plugin struct {
	client     *http.Client
	config     *Config
	avgLogSize int
	time  string
	batcher    *pipeline.Batcher
	controller pipeline.OutputPluginController
	mu         *sync.Mutex
}

type data struct {
	outBuf []byte
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginInfo{
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
		logger.Fatalf("hosts isn't set for elasticsearch output")
	}

	p.config.endpoints = strings.Split(p.config.Endpoints, ",")
	for i, endpoint := range p.config.endpoints {
		p.config.endpoints[i] = path.Join(endpoint, "/_bulk?_source=false")
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

	if p.config.TimeField == "" {
		p.config.TimeField = "ts"
	}

	if p.config.TimeFormat == "" {
		p.config.TimeFormat = "2006-01-02"
	}

	p.client = &http.Client{
		Timeout: p.config.ConnectionTimeout.Duration,
	}

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

	outBuf := data.outBuf[:0]
	for _, event := range batch.Events {
		outBuf = append(outBuf, `{"index":{"_index":"`...)

		reps := 0
		for _, c := range p.config.IndexFormat {
			if c!='%' {
				outBuf = append(outBuf, byte(c))
				continue
			}

			if reps >= len(p.config.IndexValues) {
				logger.Fatalf("wrong index format / values ")
			}
			value := p.config.IndexValues[reps]
		}

		outBuf = append(outBuf, p.indexName...)
		outBuf = append(outBuf, "\"}}\n"...)
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, '\n')
	}
	data.outBuf = outBuf

	for {
		endpoint := p.config.endpoints[rand.Int()%len(p.config.endpoints)]
		resp, err := p.client.Post(endpoint, "application/x-ndjson", bytes.NewBuffer(outBuf))
		if err == nil {
			break
		}

		logger.Errorf("can't send batch to %s, will try other endpoint", endpoint)
	}
}

func (p *Plugin) maintenance(workerData *pipeline.WorkerData) {
	p.mu.Lock()
	p.time = time.Now().Format(p.config.TimeFormat)

	values := []string{}
	for _, x := range p.config.IndexValues {
		if x == "@yyyy-mm-dd" {
			values = append(values, time.Now().Format("2006-01-02"))
		} else {
			values = append(values, time.Now().Format("2006-01-02"))
		}
	}
	p.indexName = fmt.Sprintf("%s-%s-%s-%s")
	p.mu.Unlock()
}
