package prometheus

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/castai/promwrite"
	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends metrics to Prometheus using the remote write API. The plugin receives metric events from the pipeline (e.g., from the event_to_metrics action plugin) and forwards them to a Prometheus-compatible endpoint.
}*/

const (
	outPluginType = "prometheus"
)

type data struct {
	outBuf []byte
}

type Label struct {
	Label string `json:"label" required:"true"`
	Value string `json:"value" required:"true"`
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Prometheus remote write endpoint URL.
	Endpoint string `json:"endpoint" required:"true" default:"http://localhost:9090/api/v1/write"` // *

	// > @3@4@5@6
	// >
	// > Auth config.
	// >
	// > `AuthConfig` params:
	// > * `strategy` describes strategy to use; options:"disabled|tenant|basic|bearer"
	// > By default strategy is `disabled`.
	// > * `tenant_id` should be provided if strategy is `tenant`.
	// > * `username` should be provided if strategy is `basic`.
	// > Username is used for HTTP Basic Authentication.
	// > * `password` should be provided if strategy is `basic`.
	// > Password is used for HTTP Basic Authentication.
	// > * `bearer_token` should be provided if strategy is `bearer`.
	// > Token is used for HTTP Bearer Authentication.
	Auth AuthConfig `json:"auth" child:"true"` // *

	// > @3@4@5@6
	// >
	// > If set true, the plugin will use SSL/TLS connections method.
	TLSEnabled bool `json:"tls_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will skip SSL/TLS verification.
	TLSSkipVerify bool `json:"tls_skip_verify" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Client timeout when sends requests to Prometheus HTTP API.
	RequestTimeout  cfg.Duration `json:"request_timeout" default:"1s" parse:"duration"` // *
	RequestTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > It defines how much time to wait for the connection.
	ConnectionTimeout  cfg.Duration `json:"connection_timeout" default:"5s" parse:"duration"` // *
	ConnectionTimeout_ time.Duration

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
	// > Retries of upload. If File.d cannot upload for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry to upload.
	Retention  cfg.Duration `json:"retention" default:"1s" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > Number of retry attempts.
	AttemptNum int `json:"attempt_num" default:"3"` // *
}

type AuthStrategy byte

const (
	StrategyDisabled AuthStrategy = iota
	StrategyTenant
	StrategyBasic
	StrategyBearer
)

// ! config-params
// ^ config-params
type AuthConfig struct {
	// > AuthStrategy.Strategy describes strategy to use.
	Strategy  string `json:"strategy" default:"disabled" options:"disabled|tenant|basic|bearer"`
	Strategy_ AuthStrategy

	// > TenantID for Tenant Authentication.
	TenantID string `json:"tenant_id"`

	// > Username for HTTP Basic Authentication.
	Username string `json:"username"`

	// > Password for HTTP Basic Authentication.
	Password string `json:"password"`

	// > Token for HTTP Bearer Authentication.
	BearerToken string `json:"bearer_token"`
}

type KeepAliveConfig struct {
	// Idle keep-alive connections are closed after this duration.
	MaxIdleConnDuration  cfg.Duration `json:"max_idle_conn_duration" parse:"duration" default:"10s"`
	MaxIdleConnDuration_ time.Duration

	// Keep-alive connections are closed after this duration.
	MaxConnDuration  cfg.Duration `json:"max_conn_duration" parse:"duration" default:"5m"`
	MaxConnDuration_ time.Duration
}

type Plugin struct {
	controller pipeline.OutputPluginController
	logger     *zap.Logger

	config *Config
	client PrometheusClient

	// plugin metrics
	sendErrorMetric *metric.Counter

	collector *metricCollector

	// isAvailable tracks if Prometheus is currently available
	isAvailable bool

	// retryChan is used to signal waiting events when Prometheus becomes available
	retryChan chan struct{}
}

type PrometheusClient interface {
	Write(ctx context.Context, req *promwrite.WriteRequest, options ...promwrite.WriteOption) (*promwrite.WriteResponse, error)
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
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.registerMetrics(params.MetricCtl)
	p.collector = newCollector(p, 15*time.Second, p.logger)

	p.prepareClient()
	p.isAvailable = true
	p.retryChan = make(chan struct{})
}

func (p *Plugin) Stop() {
	p.collector.shutdown()
}

func (p *Plugin) Out(event *pipeline.Event) {
	msg := event.Root
	nameNode := msg.Dig("name")
	if nameNode == nil {
		if !p.isAvailable {
			<-p.retryChan // Block until Prometheus is available
		}
		p.controller.Commit(event)
		return
	}
	name := nameNode.AsString()
	nameNode.Suicide()

	typeNode := msg.Dig("type")
	metricType := typeNode.AsString()
	typeNode.Suicide()

	timestampNode := msg.Dig("timestamp")
	timestamp := timestampNode.AsInt64()
	timestampNode.Suicide()

	ttlNode := msg.Dig("ttl")
	ttl := ttlNode.AsInt64()
	ttlNode.Suicide()

	valueNode := msg.Dig("value")
	value := valueNode.AsFloat()
	valueNode.Suicide()

	labelsNode := msg.Dig("labels")
	labelValues := labelsNode.AsFields()
	labelsNode.Suicide()

	labels := make([]promwrite.Label, 0, len(labelValues)+1)
	labels = append(labels, promwrite.Label{
		Name:  "__name__",
		Value: name,
	})

	for _, l := range labelValues {
		labels = append(labels, promwrite.Label{
			Name:  l.AsString(),
			Value: labelsNode.Dig(l.AsString()).AsString(),
		})
	}

	p.collector.handleMetric(
		labels,
		value,
		timestamp,
		metricType,
		ttl,
	)
}

func (p *Plugin) sendToStorage(values []promwrite.TimeSeries) error {
	for _, value := range values {
		p.logger.Debug(
			"send metric",
			zap.Any("labels", value.Labels),
			zap.Time("time", value.Sample.Time),
			zap.Float64("value", value.Sample.Value),
		)
	}

	expBackoff := pipeline.GetBackoff(
		p.config.Retention_,
		float64(p.config.RetentionExponentMultiplier),
		uint64(p.config.AttemptNum),
	)
	expBackoff.Reset()

	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), p.config.RequestTimeout_)
		defer cancel()
		resp, err := p.client.Write(ctx, &promwrite.WriteRequest{TimeSeries: values})
		if err != nil {
			p.sendErrorMetric.Inc()
			p.logger.Error("can't send data to Prometheus", zap.String("address", p.config.Endpoint), zap.Error(err))
			return err
		}
		p.logger.Debug("successfully sent", zap.Any("response", resp))
		return nil
	}, expBackoff)

	if err != nil {
		var level zapcore.Level
		if p.config.FatalOnFailedInsert {
			level = zapcore.FatalLevel
		} else {
			level = zapcore.ErrorLevel
		}

		p.logger.Log(level, "max retries reached")
		p.isAvailable = false
		newRetryChan := make(chan struct{})
		if p.retryChan != nil {
			close(p.retryChan)
		}
		p.retryChan = newRetryChan

		p.logger.Info("prometheus unavailable, events will wait")
	} else {
		p.isAvailable = true
		if p.retryChan != nil {
			close(p.retryChan)
			p.logger.Info("prometheus available")
		}
		p.retryChan = make(chan struct{})
	}

	return err
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_prometheus_send_error", "Total Prometheus send errors")
}

func (p *Plugin) prepareClient() {
	customClient := &http.Client{
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   p.config.ConnectionTimeout_,
				KeepAlive: p.config.KeepAlive.MaxConnDuration_,
			}).DialContext,
			IdleConnTimeout: p.config.KeepAlive.MaxIdleConnDuration_,
		},
	}

	p.client = promwrite.NewClient(p.config.Endpoint, promwrite.HttpClient(customClient))
}
