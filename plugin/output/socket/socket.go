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
	"github.com/ozontech/file.d/xtls"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends events to a socket endpoint.
Supports TCP, UDP, and Unix socket protocols.

Events are sent in batches serialized as newline-delimited JSON, compatible with the socket input plugin.
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
	// > It defines how much time to wait for the connection.
	DialTimeout  cfg.Duration `json:"dial_timeout" default:"5s" parse:"duration"` // *
	DialTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > Timeout for writing a single batch to the socket.
	// >> Set to `0` to disable.
	WriteTimeout  cfg.Duration `json:"write_timeout" default:"5s" parse:"duration"` // *
	WriteTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how often to perform maintenance
	MaintenanceInterval  cfg.Duration `json:"maintenance_interval" default:"1m" parse:"duration"` // *
	MaintenanceInterval_ time.Duration

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

type KeepAliveConfig struct {
	// Idle keep-alive connections are closed after this duration.
	MaxIdleConnDuration  cfg.Duration `json:"max_idle_conn_duration" parse:"duration" default:"10s"`
	MaxIdleConnDuration_ time.Duration

	// Keep-alive connections are closed after this duration.
	MaxConnDuration  cfg.Duration `json:"max_conn_duration" parse:"duration" default:"5m"`
	MaxConnDuration_ time.Duration
}

type data struct {
	conn   net.Conn
	outBuf []byte

	connCreatedTime  time.Time
	connLastUsedTime time.Time
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

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:        params.PipelineName,
		OutputType:          outPluginType,
		MaintenanceFn:       p.maintenance,
		Controller:          params.Controller,
		Workers:             p.config.WorkersCount_,
		BatchSizeCount:      p.config.BatchSize_,
		BatchSizeBytes:      p.config.BatchSizeBytes_,
		FlushTimeout:        p.config.BatchFlushTimeout_,
		MaintenanceInterval: p.config.MaintenanceInterval_,
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

func (p *Plugin) maintenance(wd *pipeline.WorkerData) {
	if *wd == nil {
		return
	}
	d := (*wd).(*data)
	if d.conn == nil {
		return
	}

	now := time.Now()
	if p.config.KeepAlive.MaxIdleConnDuration_ > 0 && now.Sub(d.connLastUsedTime) >= p.config.KeepAlive.MaxIdleConnDuration_ {
		p.logger.Info("closing idle socket connection",
			zap.Duration("idle_for", now.Sub(d.connLastUsedTime)),
			zap.Duration("max_idle_conn_duration", p.config.KeepAlive.MaxIdleConnDuration_),
		)
		_ = d.conn.Close()
		d.conn = nil
		return
	}

	if p.config.KeepAlive.MaxConnDuration_ > 0 && now.Sub(d.connCreatedTime) >= p.config.KeepAlive.MaxConnDuration_ {
		p.logger.Info("closing long-lived socket connection",
			zap.Duration("alive_for", now.Sub(d.connCreatedTime)),
			zap.Duration("max_conn_duration", p.config.KeepAlive.MaxConnDuration_),
		)
		_ = d.conn.Close()
		d.conn = nil
	}
}

func (p *Plugin) out(wd *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *wd == nil {
		*wd = &data{
			outBuf: make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}
	d := (*wd).(*data)

	if cap(d.outBuf) > p.config.BatchSize_*p.avgEventSize {
		d.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	if d.conn != nil && p.config.KeepAlive.MaxConnDuration_ > 0 && time.Since(d.connCreatedTime) >= p.config.KeepAlive.MaxConnDuration_ {
		p.logger.Info("closing socket connection: max_conn_duration exceeded",
			zap.Duration("alive_for", time.Since(d.connCreatedTime)),
			zap.Duration("max_conn_duration", p.config.KeepAlive.MaxConnDuration_),
		)
		_ = d.conn.Close()
		d.conn = nil
	}

	if d.conn == nil {
		conn, err := p.dial()
		if err != nil {
			p.sendErrorMetric.WithLabelValues("connect").Inc()
			p.logger.Error("can't connect to the socket endpoint", zap.Error(err),
				zap.String("network", p.config.Network),
				zap.String("address", p.config.Address),
			)
			return err
		}
		now := time.Now()
		d.conn = conn
		d.connCreatedTime = now
		d.connLastUsedTime = now
	}

	d.outBuf = d.outBuf[:0]
	batch.ForEach(func(event *pipeline.Event) {
		d.outBuf, _ = event.Encode(d.outBuf)
		d.outBuf = append(d.outBuf, '\n')
	})

	if p.config.WriteTimeout_ > 0 {
		_ = d.conn.SetWriteDeadline(time.Now().Add(p.config.WriteTimeout_))
	}

	if err := writeAll(d.conn, d.outBuf); err != nil {
		p.sendErrorMetric.WithLabelValues("write").Inc()
		p.logger.Error("can't write batch to the socket endpoint", zap.Error(err),
			zap.String("network", p.config.Network),
			zap.String("address", p.config.Address),
		)
		_ = d.conn.Close()
		d.conn = nil
		return err
	}

	d.connLastUsedTime = time.Now()

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
