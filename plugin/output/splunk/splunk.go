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

	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/consts"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It sends events to splunk.
}*/

const (
	outPluginType = "splunk"
	subsystemName = "output_splunk"

	sendErrorCounter = "send_error"
)

type Plugin struct {
	config       *Config
	client       http.Client
	logger       *zap.SugaredLogger
	avgEventSize int
	batcher      *pipeline.Batcher
	controller   pipeline.OutputPluginController
	backoff      backoff.BackOff
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> A full URI address of splunk HEC endpoint. Format: `http://127.0.0.1:8088/services/collector`.
	Endpoint string `json:"endpoint" required:"true"` //*

	//> @3@4@5@6
	//>
	//> Token for an authentication for a HEC endpoint.
	Token string `json:"token" required:"true"` //*

	//> @3@4@5@6
	//>
	//> How many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3@4@5@6
	//>
	//> Client timeout when sends requests to HTTP Event Collector.
	RequestTimeout  cfg.Duration `json:"request_timeout" default:"1s" parse:"duration"` //*
	RequestTimeout_ time.Duration

	//> @3@4@5@6
	//>
	//> A maximum quantity of events to pack into one batch.
	BatchSize  cfg.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> @3@4@5@6
	//>
	//> After this timeout the batch will be sent even if batch isn't completed.
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
	p.client = p.newClient(p.config.RequestTimeout_)

	ctx := context.TODO()
	stdBackoff := backoff.NewExponentialBackOff()
	stdBackoff.Multiplier = consts.ExpBackoffDefaultMultiplier
	stdBackoff.RandomizationFactor = consts.ExpBackoffDefaultRndFactor
	stdBackoff.InitialInterval = time.Second
	stdBackoff.MaxInterval = stdBackoff.InitialInterval * 2
	p.backoff = backoff.WithContext(stdBackoff, ctx)

	p.registerPluginMetrics()

	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		outPluginType,
		p.out,
		p.maintenance,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		0,
	)

	p.batcher.Start(ctx)
}

func (p *Plugin) registerPluginMetrics() {
	stats.RegisterCounter(&stats.MetricDesc{
		Name:      sendErrorCounter,
		Subsystem: subsystemName,
		Help:      "Total splunk send errors",
	})
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
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

	root := insaneJSON.Spawn()
	outBuf := data.outBuf[:0]

	for _, event := range batch.Events {
		root.AddField("event").MutateToNode(event.Root.Node)
		outBuf = root.Encode(outBuf)
		_ = root.DecodeString("{}")
	}
	insaneJSON.Release(root)
	data.outBuf = outBuf

	p.logger.Debugf("Trying to send: %s", outBuf)
	_ = backoff.Retry(func() error {
		sendErr := p.send(outBuf)
		if sendErr != nil {
			stats.GetCounter(subsystemName, sendErrorCounter).Inc()
			p.logger.Errorf("can't send data to splunk address=%s: %s", p.config.Endpoint, sendErr.Error())

			return sendErr
		}
		return nil
	}, p.backoff)
	p.logger.Debugf("Successfully sent: %s", outBuf)
}

func (p *Plugin) maintenance(workerData *pipeline.WorkerData) {}

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
	req, err := http.NewRequestWithContext(context.Background(), "POST", p.config.Endpoint, r)
	if err != nil {
		return fmt.Errorf("can't create request: %w", err)
	}

	req.Header.Set("Authorization", "Splunk "+p.config.Token)
	resp, err := p.client.Do(req)
	if err != nil {
		return fmt.Errorf("can't send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
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
