package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xoauth"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It sends the event batches to kafka brokers using `franz-go` lib.

Supports [dead queue](/plugin/output/README.md#dead-queue).
}*/

const (
	outPluginType = "kafka"
)

type data struct {
	messages []*kgo.Record
	outBuf   []byte
}

type Plugin struct {
	logger       *zap.Logger
	config       *Config
	avgEventSize int
	controller   pipeline.OutputPluginController
	cancel       context.CancelFunc

	client      KafkaClient
	batcher     *pipeline.RetriableBatcher
	tokenSource xoauth.TokenSource

	// plugin metrics
	sendErrorMetric prometheus.Counter
	router          *pipeline.Router
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > List of kafka brokers to write to.
	Brokers []string `json:"brokers" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.
	DefaultTopic string `json:"default_topic" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Kafka client ID.
	ClientID string `json:"client_id" default:"file-d"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use topic name from the event field.
	UseTopicField bool `json:"use_topic_field" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Which event field to use as topic name. It works only if `should_use_topic_field` is set.
	TopicField string `json:"topic_field" default:"topic"` // *

	// > @3@4@5@6
	// >
	// > How many workers will be instantiated to send batches.
	WorkersCount  cfg.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` // *
	WorkersCount_ int

	// > @3@4@5@6
	// >
	// > A maximum quantity of the events to pack into one batch.
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
	// > After this timeout the batch will be sent even if batch isn't full.
	BatchFlushTimeout  cfg.Duration `json:"batch_flush_timeout" default:"200ms" parse:"duration"` // *
	BatchFlushTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > The maximum permitted size of a message.
	// > Should be set equal to or smaller than the broker's `message.max.bytes`.
	MaxMessageBytes  cfg.Expression `json:"max_message_bytes" default:"1000000" parse:"expression"` // *
	MaxMessageBytes_ int

	// > @3@4@5@6
	// >
	// > Compression codec
	Compression string `json:"compression" default:"none" options:"none|gzip|snappy|lz4|zstd"` // *

	// > @3@4@5@6
	// >
	// > Required acks for produced records
	Ack string `json:"ack" default:"leader" options:"no|leader|all-isr"` // *

	// > @3@4@5@6
	// >
	// > Retries of insertion. If File.d cannot insert for this number of attempts,
	// > File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).
	// >
	// > There are situations when one of the brokers is disconnected and the client does not have time to
	// > update the metadata before all the remaining retries are finished. To avoid this situation,
	// > the client.ForceMetadataRefresh() function is used for some ProduceSync errors:
	// > - kerr.LeaderNotAvailable - There is no leader for this topic-partition as we are in the middle of a leadership election.
	// > - kerr.NotLeaderForPartition - This server is not the leader for that topic-partition.
	Retry int `json:"retry" default:"10"` // *

	// > @3@4@5@6
	// >
	// > After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.
	FatalOnFailedInsert bool `json:"fatal_on_failed_insert" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Retention milliseconds for retry.
	Retention  cfg.Duration `json:"retention" default:"50ms" parse:"duration"` // *
	Retention_ time.Duration

	// > @3@4@5@6
	// >
	// > Multiplier for exponential increase of retention between retries
	RetentionExponentMultiplier int `json:"retention_exponentially_multiplier" default:"2"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SASL authentications mechanism.
	SaslEnabled bool `json:"is_sasl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > SASL mechanism to use.
	SaslMechanism string `json:"sasl_mechanism" default:"SCRAM-SHA-512" options:"PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|AWS_MSK_IAM|OAUTHBEARER"` // *

	// > @3@4@5@6
	// >
	// > SASL username.
	SaslUsername string `json:"sasl_username" default:"user"` // *

	// > @3@4@5@6
	// >
	// > SASL password.
	SaslPassword string `json:"sasl_password" default:"password"` // *

	// > @3@4@5@6
	// >
	// > SASL OAUTHBEARER config. It works only if `sasl_mechanism:"OAUTHBEARER"`.
	// >> There are 2 options - a static token or a dynamically updated.
	// >
	// > `OAuthConfig` params:
	// > * **`token`** *`string`* - static token
	// > ---
	// > * **`client_id`** *`string`* - client ID
	// > * **`client_secret`** *`string`* - client secret
	// > * **`token_url`** *`string`* - resource server's token endpoint URL
	// > * **`scopes`** *`[]string`* - optional requested permissions
	// > * **`auth_style`** *`string`* - specifies how the endpoint wants the client ID & client secret sent
	SaslOAuth cfg.KafkaClientOAuthConfig `json:"sasl_oauth" child:"true"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SSL/TLS connections method.
	SslEnabled bool `json:"is_ssl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will skip SSL/TLS verification.
	SslSkipVerify bool `json:"ssl_skip_verify" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Path or content of a PEM-encoded client certificate file.
	ClientCert string `json:"client_cert"` // *

	// > @3@4@5@6
	// >
	// > > Path or content of a PEM-encoded client key file.
	ClientKey string `json:"client_key"` // *

	// > @3@4@5@6
	// >
	// > Path or content of a PEM-encoded CA file.
	CACert string `json:"ca_cert"` // *
}

func (c *Config) GetBrokers() []string {
	return c.Brokers
}

func (c *Config) GetClientID() string {
	return c.ClientID
}

func (c *Config) IsSaslEnabled() bool {
	return c.SaslEnabled
}

func (c *Config) GetSaslConfig() cfg.KafkaClientSaslConfig {
	return cfg.KafkaClientSaslConfig{
		SaslMechanism: c.SaslMechanism,
		SaslUsername:  c.SaslUsername,
		SaslPassword:  c.SaslPassword,
		SaslOAuth:     c.SaslOAuth,
	}
}

func (c *Config) IsSslEnabled() bool {
	return c.SslEnabled
}

func (c *Config) GetSslConfig() cfg.KafkaClientSslConfig {
	return cfg.KafkaClientSslConfig{
		CACert:        c.CACert,
		ClientCert:    c.ClientCert,
		ClientKey:     c.ClientKey,
		SslSkipVerify: c.SslSkipVerify,
	}
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
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.controller = params.Controller
	p.registerMetrics(params.MetricCtl)

	if p.config.Retention_ < 1 {
		p.logger.Fatal("'retention' can't be <1")
	}

	p.logger.Info(fmt.Sprintf("workers count=%d, batch size=%d", p.config.WorkersCount_, p.config.BatchSize_))

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel

	var err error
	p.tokenSource, err = cfg.GetKafkaClientOAuthTokenSource(ctx, p.config)
	if err != nil {
		p.logger.Fatal(err.Error())
	}

	p.client = NewClient(ctx, p.config, p.logger, p.tokenSource)

	batcherOpts := pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		Controller:     p.controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
		MetricCtl:      params.MetricCtl,
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

		p.logger.Log(level, "can't write batch",
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

	p.batcher.Start(context.TODO())
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_kafka_send_errors_total", "Total Kafka send errors")
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) error {
	if *workerData == nil {
		*workerData = &data{
			messages: make([]*kgo.Record, p.config.BatchSize_),
			outBuf:   make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make([]byte, 0, p.config.BatchSize_*p.avgEventSize)
	}

	outBuf := data.outBuf[:0]
	start := 0
	i := 0
	batch.ForEach(func(event *pipeline.Event) {
		outBuf, start = event.Encode(outBuf)

		topic := p.config.DefaultTopic
		if p.config.UseTopicField {
			fieldValue := event.Root.Dig(p.config.TopicField).AsString()
			if fieldValue != "" {
				topic = pipeline.CloneString(fieldValue)
			}
		}

		if data.messages[i] == nil {
			data.messages[i] = &kgo.Record{}
		}
		data.messages[i].Timestamp = time.Now()
		data.messages[i].Value = outBuf[start:]
		data.messages[i].Topic = topic
		i++
	})

	if err := p.client.ProduceSync(context.Background(), data.messages[:i]...).FirstErr(); err != nil {
		if errors.Is(err, kerr.LeaderNotAvailable) || errors.Is(err, kerr.NotLeaderForPartition) {
			p.client.ForceMetadataRefresh()
		}
		p.logger.Error("can't write batch", zap.Error(err))
		p.sendErrorMetric.Inc()
		return err
	}

	return nil
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
	p.client.Close()
	if p.tokenSource != nil {
		p.tokenSource.Stop()
	}
	p.cancel()
}
