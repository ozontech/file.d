package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xscram"
	"github.com/ozontech/file.d/xtls"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.

**Example**
Standard example:
```yaml
pipelines:
  example_pipeline:
    input:
      type: kafka
      brokers: [kafka:9092, kafka:9091]
      topics: [topic1, topic2]
      offset: newest
      meta:
        partition: '{{ .partition }}'
        topic: '{{ .topic }}'
        offset: '{{ .offset }}'
    # output plugin is not important in this case, let's emulate s3 output.
    output:
      type: s3
      file_config:
        retention_interval: 10s
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
```
}*/

type Plugin struct {
	config        *Config
	logger        *zap.SugaredLogger
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	controller    pipeline.InputPluginController
	idByTopic     map[string]int

	// plugin metrics
	commitErrorsMetric  prometheus.Counter
	consumeErrorsMetric prometheus.Counter

	metaRegistry *pipeline.MetaTemplater
}

type OffsetType byte

const (
	OffsetTypeNewest OffsetType = iota
	OffsetTypeOldest
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The name of kafka brokers to read from.
	Brokers []string `json:"brokers" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The list of kafka topics to read from.
	Topics []string `json:"topics" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The name of consumer group to use.
	ConsumerGroup string `json:"consumer_group" default:"file-d"` // *

	// > @3@4@5@6
	// >
	// > Kafka client ID.
	ClientID string `json:"client_id" default:"file-d"` // *

	// > @3@4@5@6
	// >
	// > The number of unprocessed messages in the buffer that are loaded in the background from kafka.
	ChannelBufferSize int `json:"channel_buffer_size" default:"256"` // *

	// > @3@4@5@6
	// >
	// > The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
	// > * *`newest`* - set offset to the newest message
	// > * *`oldest`* - set offset to the oldest message
	Offset  string `json:"offset" default:"newest" options:"newest|oldest"` // *
	Offset_ OffsetType

	// > @3@4@5@6
	// >
	// > The maximum amount of time the consumer expects a message takes to process for the user.
	ConsumerMaxProcessingTime  cfg.Duration `json:"consumer_max_processing_time" default:"200ms" parse:"duration"` // *
	ConsumerMaxProcessingTime_ time.Duration

	// > @3@4@5@6
	// >
	// > The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways.
	ConsumerMaxWaitTime  cfg.Duration `json:"consumer_max_wait_time" default:"250ms" parse:"duration"` // *
	ConsumerMaxWaitTime_ time.Duration

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SASL authentications mechanism.
	SaslEnabled bool `json:"is_sasl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > SASL mechanism to use.
	SaslMechanism string `json:"sasl_mechanism" default:"SCRAM-SHA-512" options:"PLAIN|SCRAM-SHA-256|SCRAM-SHA-512"` // *

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

	// > @3@4@5@6
	// >
	// > Meta params
	// >
	// > Add meta information to an event (look at Meta params)
	// >
	// > Example: ```topic: '{{ .topic }}'```
	Meta cfg.MetaTemplates `json:"meta"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "kafka",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
	p.logger = params.Logger
	p.config = config.(*Config)
	p.registerMetrics(params.MetricCtl)
	p.metaRegistry = pipeline.NewMetaTemplater(p.config.Meta)

	p.idByTopic = make(map[string]int, len(p.config.Topics))
	for i, topic := range p.config.Topics {
		p.idByTopic[topic] = i
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.consumerGroup = NewConsumerGroup(p.config, p.logger)
	p.controller.UseSpread()
	p.controller.DisableStreams()

	go p.consume(ctx)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.commitErrorsMetric = ctl.RegisterCounter("input_kafka_commit_errors", "Number of kafka commit errors")
	p.consumeErrorsMetric = ctl.RegisterCounter("input_kafka_consume_errors", "Number of kafka consume errors")
}

func (p *Plugin) consume(ctx context.Context) {
	p.logger.Infof("kafka input reading from topics: %s", strings.Join(p.config.Topics, ","))
	for {
		err := p.consumerGroup.Consume(ctx, p.config.Topics, p)
		if err != nil {
			p.consumeErrorsMetric.Inc()
			p.logger.Errorf("can't consume from kafka: %s", err.Error())
		}

		if ctx.Err() != nil {
			return
		}
	}
}

func (p *Plugin) Stop() {
	p.cancel()
}

func (p *Plugin) Commit(event *pipeline.Event) {
	session := p.session
	if session == nil {
		p.commitErrorsMetric.Inc()
		p.logger.Errorf("no kafka consumer session for event commit")
		return
	}
	index, partition := disassembleSourceID(event.SourceID)
	session.MarkOffset(p.config.Topics[index], partition, event.Offset+1, "")
}

func NewConsumerGroup(c *Config, l *zap.SugaredLogger) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.ClientID = c.ClientID

	// kafka auth sasl
	if c.SaslEnabled {
		config.Net.SASL.Enable = true

		config.Net.SASL.User = c.SaslUsername
		config.Net.SASL.Password = c.SaslPassword

		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SaslMechanism)
		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return xscram.NewClient(xscram.SHA256) }
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return xscram.NewClient(xscram.SHA512) }
		}
	}

	// kafka connect via SSL with PEM
	if c.SslEnabled {
		config.Net.TLS.Enable = true

		tlsCfg := xtls.NewConfigBuilder()

		if c.CACert != "" {
			if err := tlsCfg.AppendCARoot(c.CACert); err != nil {
				l.Fatalf("can't load ca cert: %s", err.Error())
			}
		}
		tlsCfg.SetSkipVerify(c.SslSkipVerify)

		if c.ClientCert != "" || c.ClientKey != "" {
			if err := tlsCfg.AppendX509KeyPair(c.ClientCert, c.ClientKey); err != nil {
				l.Fatalf("can't load client certificate and key: %s", err.Error())
			}
		}

		config.Net.TLS.Config = tlsCfg.Build()
	}

	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V0_10_2_0
	config.ChannelBufferSize = c.ChannelBufferSize
	config.Consumer.MaxProcessingTime = c.ConsumerMaxProcessingTime_
	config.Consumer.MaxWaitTime = c.ConsumerMaxWaitTime_

	switch c.Offset_ {
	case OffsetTypeOldest:
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	case OffsetTypeNewest:
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	default:
		l.Fatalf("unexpected value of the offset field: %s", c.Offset)
	}

	consumerGroup, err := sarama.NewConsumerGroup(c.Brokers, c.ConsumerGroup, config)
	if err != nil {
		l.Fatalf("can't create kafka consumer: %s", err.Error())
	}

	return consumerGroup
}

func (p *Plugin) Setup(session sarama.ConsumerGroupSession) error {
	p.logger.Infof("kafka consumer created with brokers %q", strings.Join(p.config.Brokers, ","))
	p.session = session
	return nil
}

func (p *Plugin) Cleanup(sarama.ConsumerGroupSession) error {
	p.session = nil
	return nil
}

func (p *Plugin) ConsumeClaim(_ sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		sourceID := assembleSourceID(p.idByTopic[message.Topic], message.Partition)

		var metadataInfo pipeline.MetaData
		var err error
		if len(p.config.Meta) > 0 {
			metadataInfo, err = p.metaRegistry.Render(newMetaInformation(message))
			if err != nil {
				return err
			}
		}

		_ = p.controller.In(sourceID, "kafka", message.Offset, message.Value, true, metadataInfo)
	}

	return nil
}

func assembleSourceID(index int, partition int32) pipeline.SourceID {
	return pipeline.SourceID(index<<16 + int(partition))
}

func disassembleSourceID(sourceID pipeline.SourceID) (index int, partition int32) {
	index = int(sourceID >> 16)
	partition = int32(sourceID & 0xFFFF)

	return
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}

type MetaInformation struct {
	Topic     string
	Partition int32
	Offset    int64
}

func newMetaInformation(message *sarama.ConsumerMessage) MetaInformation {
	return MetaInformation{
		Topic:     message.Topic,
		Partition: message.Partition,
		Offset:    message.Offset,
	}
}

func (m MetaInformation) GetData() map[string]interface{} {
	return map[string]interface{}{
		"topic":     m.Topic,
		"partition": m.Partition,
		"offset":    m.Offset,
	}
}
