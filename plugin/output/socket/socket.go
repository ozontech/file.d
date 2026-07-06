package socket

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtime"
	"github.com/ozontech/file.d/xtls"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends events to a socket endpoint.
Supports TCP, UDP, and Unix socket protocols.

Events are sent in batches serialized as newline-delimited JSON by default, compatible with the socket input plugin.
The delimiter used to separate messages is configurable and can be changed in the plugin configuration (default: `\n`).
If a network error occurs, the batch will be retried according to the backoff settings.

Supports [dead queue](/plugin/output/README.md#dead-queue).
}*/

/*{ examples
TCP:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: tcp
      address: ':6666'
      delimiter: '\t'
    ...
```
---
TLS:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: tcp
      address: ':6666'
      delimiter: '\n'
      ca_cert: './client.pem'
      private_key: './client.key'
    ...
```
---
UDP:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: udp
      address: '[2001:db8::1]:1234'
    ...
```
---
Unix:
```yaml
pipelines:
  example_pipeline:
    ...
    output:
      type: socket
      network: unix
      address: '/tmp/filed.sock'
    ...
```
}*/

const (
	outPluginType = "socket"

	networkTCP  = "tcp"
	networkUDP  = "udp"
	networkUnix = "unix"
)

type Plugin struct {
	logger *zap.Logger

	config       *Config
	tlsConfig    *tls.Config
	batcher      *pipeline.RetriableBatcher
	avgEventSize int
	cancel       context.CancelFunc

	sendErrorMetric *metric.CounterVec

	router *pipeline.Router
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Which network protocol to use for the outgoing connection.
	Network string `json:"network" default:"tcp" options:"tcp|udp|unix"` // *

	// > @3@4@5@6
	// >
	// > Remote address to connect to.
	// >
	// > Examples:
	// > - 1.2.3.4:6666
	// > - :6666
	// > - /tmp/filed.sock
	Address string `json:"address" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Delimiter to append after each event. Must be exactly one byte.
	Delimiter  string `json:"delimiter" default:"\n"` // *
	Delimiter_ byte

	// > @3@4@5@6
	// >
	// > Client certificate in PEM encoding. This can be a path or the contents of the file.
	// >> Enables TLS. Works only when `network` is `tcp`.
	CACert string `json:"ca_cert" default:""` // *

	// > @3@4@5@6
	// >
	// > Client private key in PEM encoding. This can be a path or the contents of the file.
	// >> Enables TLS. Works only when `network` is `tcp`.
	PrivateKey string `json:"private_key" default:""` // *

	// > @3@4@5@6
	// >
	// > It defines how much time to wait for the connection.
	DialTimeout  cfg.Duration `json:"dial_timeout" default:"5s" parse:"duration"` // *
	DialTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Timeout for writing a single batch to the socket.
	// >> Set to `0` to disable.
	// >> **Must be at least `1s` if non-zero.**
	WriteTimeout  cfg.Duration `json:"write_timeout" default:"5s" parse:"duration"` // *
	WriteTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > The plugin reconnects to endpoint periodically using this interval. It is useful if an endpoint is a load balancer.
	ReconnectInterval  cfg.Duration `json:"reconnect_interval" default:"1m" parse:"duration"` // *
	ReconnectInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

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
	// > After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to socket.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *
}

type data struct {
	conn   net.Conn
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
	p.logger = params.Logger.Desugar()
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)

	p.buildTLSConfig()
	p.validateDelimiter()

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:        params.PipelineName,
		OutputType:          outPluginType,
		MaintenanceFn:       p.maintenance,
		Controller:          params.Controller,
		Workers:             p.config.WorkersCount_,
		BatchSizeCount:      p.config.BatchSize_,
		BatchSizeBytes:      p.config.BatchSizeBytes_,
		FlushTimeout:        p.config.BatchFlushTimeout_,
		MaintenanceInterval: p.config.ReconnectInterval_,
		MetricCtl:           params.MetricCtl,
	}

	p.router = params.Router
	backoffOpts := pipeline.BackoffOpts{
		MinRetention:         p.config.Retention_,
		Multiplier:           float64(p.config.RetentionExponentMultiplier),
		AttemptNum:           p.config.Retry,
		IsDeadQueueAvailable: p.router.IsDeadQueueAvailable(),
	}

	onError := func(err error, events []*pipeline.Event) {
		var level zapcore.Level
		if p.config.FatalOnFailedInsert && !p.router.IsDeadQueueAvailable() {
			level = zapcore.FatalLevel
		} else {
			level = zapcore.ErrorLevel
		}
		p.logger.Log(level, "can't send to the socket endpoint", zap.Error(err),
			zap.Int("retries", p.config.Retry),
		)
		for i := range events {
			p.router.Fail(events[i])
		}
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
		"output_socket_send_error_total",
		"Total socket send errors",
		"error_type",
	)
}

func (p *Plugin) buildTLSConfig() {
	if p.config.Network != networkTCP || p.config.CACert == "" || p.config.PrivateKey == "" {
		return
	}
	tlsBuilder := xtls.NewConfigBuilder()
	if err := tlsBuilder.AppendX509KeyPair(p.config.CACert, p.config.PrivateKey); err != nil {
		p.logger.Fatal("can't build TLS config", zap.Error(err))
	}
	p.tlsConfig = tlsBuilder.Build()
}

func (p *Plugin) validateDelimiter() {
	if len(p.config.Delimiter) != 1 {
		p.logger.Fatal("delimiter must be exactly 1 byte", zap.String("delimiter", p.config.Delimiter))
	}
	p.config.Delimiter_ = p.config.Delimiter[0]
}

func (p *Plugin) maintenance(workerData *pipeline.WorkerData) {
	if *workerData == nil {
		return
	}
	data := (*workerData).(*data)
	if data.conn == nil {
		return
	}
	_ = data.conn.Close()
	data.conn = nil
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		*workerData = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}
	data := (*workerData).(*data)

	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	if data.conn == nil {
		conn, err := p.dial()
		if err != nil {
			p.sendErrorMetric.WithLabelValues("connect").Inc()
			p.logger.Error("can't connect to the socket endpoint", zap.Error(err),
				zap.String("network", p.config.Network),
				zap.String("address", p.config.Address),
			)
			return err
		}
		data.conn = conn
	}

	outBuf := data.outBuf[:0]
	batch.ForEach(func(event *pipeline.Event) {
		outBuf, _ = event.Encode(outBuf)
		outBuf = append(outBuf, p.config.Delimiter_)
	})
	data.outBuf = outBuf

	if p.config.WriteTimeout_ > 0 {
		_ = data.conn.SetWriteDeadline(xtime.GetInaccurateTime().Add(p.config.WriteTimeout_))
	}

	if err := writeAll(data.conn, data.outBuf); err != nil {
		p.sendErrorMetric.WithLabelValues("write").Inc()
		p.logger.Error("can't write batch to the socket endpoint", zap.Error(err),
			zap.String("network", p.config.Network),
			zap.String("address", p.config.Address),
		)
		_ = data.conn.Close()
		data.conn = nil
		return err
	}

	return nil
}

func (p *Plugin) dial() (net.Conn, error) {
	switch p.config.Network {
	case networkTCP:
		d := &net.Dialer{Timeout: p.config.DialTimeout_}
		if p.tlsConfig != nil {
			return (&tls.Dialer{NetDialer: d, Config: p.tlsConfig}).Dial(networkTCP, p.config.Address)
		}
		return d.Dial(networkTCP, p.config.Address)
	case networkUnix, networkUDP:
		return (&net.Dialer{Timeout: p.config.DialTimeout_}).Dial(p.config.Network, p.config.Address)
	default:
		return nil, fmt.Errorf("unsupported network: %s", p.config.Network)
	}
}

func writeAll(conn net.Conn, data []byte) error {
	for len(data) > 0 {
		n, err := conn.Write(data)
		if err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
