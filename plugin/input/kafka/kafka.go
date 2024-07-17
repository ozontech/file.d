package kafka

import (
	"context"
	"strings"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from multiple Kafka topics using `franz-go` library.
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
	config     *Config
	logger     *zap.SugaredLogger
	client     *kgo.Client
	cancel     context.CancelFunc
	controller pipeline.InputPluginController
	idByTopic  map[string]int

	// plugin metrics
	commitErrorsMetric  prometheus.Counter
	consumeErrorsMetric prometheus.Counter

	metaTemplater *metadata.MetaTemplater
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
	// > The number of unprocessed messages in the buffer that are loaded in the background from kafka. (max.poll.records)
	ChannelBufferSize int `json:"channel_buffer_size" default:"256"` // *

	// > @3@4@5@6
	// >
	// > MaxConcurrentFetches sets the maximum number of fetch requests to allow in
	// > flight or buffered at once, overriding the unbounded (i.e. number of
	// > brokers) default.
	MaxConcurrentFetches int `json:"max_concurrent_fetches" default:"0"` // *

	// > @3@4@5@6
	// >
	// > FetchMaxBytes (fetch.max.bytes) sets the maximum amount of bytes a broker will try to send during a fetch
	FetchMaxBytes  cfg.Expression `json:"fetch_max_bytes" default:"52428800" parse:"expression"` // *
	FetchMaxBytes_ int32

	// > @3@4@5@6
	// >
	// > FetchMinBytes (fetch.min.bytes) sets the minimum amount of bytes a broker will try to send during a fetch
	FetchMinBytes  cfg.Expression `json:"fetch_min_bytes" default:"1" parse:"expression"` // *
	FetchMinBytes_ int32

	// > @3@4@5@6
	// >
	// > The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
	// > * *`newest`* - set offset to the newest message
	// > * *`oldest`* - set offset to the oldest message
	Offset  string `json:"offset" default:"newest" options:"newest|oldest"` // *
	Offset_ OffsetType

	// > @3@4@5@6
	// >
	// > Algorithm used by Kafka to assign partitions to consumers in a group.
	// > * *`round-robin`* - M0: [t0p0, t0p2, t1p1], M1: [t0p1, t1p0, t1p2]
	// > * *`range`* - M0: [t0p0, t0p1, t1p0, t1p1], M1: [t0p2, t1p2]
	// > * *`sticky`* - ensures minimal partition movement on group changes while also ensuring optimal balancing
	// > * *`cooperative-sticky`* - performs the sticky balancing strategy, but additionally opts the consumer group into "cooperative" rebalancing
	Balancer string `json:"balancer" default:"round-robin" options:"round-robin|range|sticky|cooperative-sticky"` // *

	// > @3@4@5@6
	// >
	// > The maximum amount of time the consumer expects a message takes to process for the user. (Not used anymore!)
	ConsumerMaxProcessingTime  cfg.Duration `json:"consumer_max_processing_time" default:"200ms" parse:"duration"` // *
	ConsumerMaxProcessingTime_ time.Duration

	// > @3@4@5@6
	// >
	// > The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways. (fetch.max.wait.ms)
	ConsumerMaxWaitTime  cfg.Duration `json:"consumer_max_wait_time" default:"250ms" parse:"duration"` // *
	ConsumerMaxWaitTime_ time.Duration

	// > @3@4@5@6
	// >
	// > AutoCommitInterval sets how long to go between autocommits
	AutoCommitInterval  cfg.Duration `json:"auto_commit_interval" default:"1s" parse:"duration"` // *
	AutoCommitInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > SessionTimeout sets how long a member in the group can go between heartbeats
	SessionTimeout  cfg.Duration `json:"session_timeout" default:"10s" parse:"duration"` // *
	SessionTimeout_ time.Duration

	// > @3@4@5@6
	// >
	// > HeartbeatInterval sets how long a group member goes between heartbeats to Kafka
	HeartbeatInterval  cfg.Duration `json:"heartbeat_interval" default:"3s" parse:"duration"` // *
	HeartbeatInterval_ time.Duration

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SASL authentications mechanism.
	SaslEnabled bool `json:"is_sasl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > SASL mechanism to use.
	SaslMechanism string `json:"sasl_mechanism" default:"SCRAM-SHA-512" options:"PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|AWS_MSK_IAM"` // *

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
	// > Use [go-template](https://pkg.go.dev/text/template) syntax
	// >
	// > Example: ```topic: '{{ .topic }}'```
	Meta cfg.MetaTemplates `json:"meta"` // *
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
	p.metaTemplater = metadata.NewMetaTemplater(p.config.Meta)

	p.idByTopic = make(map[string]int, len(p.config.Topics))
	for i, topic := range p.config.Topics {
		p.idByTopic[topic] = i
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.client = NewClient(p.config, p.logger.Desugar())
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
		fetches := p.client.PollRecords(ctx, p.config.ChannelBufferSize)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				p.consumeErrorsMetric.Inc()
				p.logger.Errorf("can't consume from kafka: %s", err.Err.Error())
			}
		}

		if ctx.Err() != nil {
			return
		}

		p.ConsumeClaim(fetches)
	}
}

func (p *Plugin) Stop() {
	p.logger.Infof("Stopping")
	err := p.client.CommitMarkedOffsets(context.Background())
	if err != nil {
		p.commitErrorsMetric.Inc()
		p.logger.Errorf("can't commit marked offsets: %s", err.Error())
	}
	p.client.Close()
	p.cancel()
}

func (p *Plugin) Commit(event *pipeline.Event) {
	index, partition := disassembleSourceID(event.SourceID)

	offset := disassembleOffset(event.Offset)
	offsets := map[string]map[int32]kgo.EpochOffset{
		p.config.Topics[index]: {partition: offset},
	}
	p.client.MarkCommitOffsets(offsets)
}

func (p *Plugin) ConsumeClaim(fetches kgo.Fetches) {
	fetches.EachRecord(func(message *kgo.Record) {
		sourceID := assembleSourceID(
			p.idByTopic[message.Topic],
			message.Partition,
		)

		offset := assembleOffset(message)
		var metadataInfo metadata.MetaData
		var err error
		if len(p.config.Meta) > 0 {
			metadataInfo, err = p.metaTemplater.Render(newMetaInformation(message))
			if err != nil {
				p.logger.Errorf("can't render meta data: %s", err.Error())
			}
		}

		_ = p.controller.In(sourceID, "kafka", offset, message.Value, true, metadataInfo)
	})
}

func assembleSourceID(index int, partition int32) pipeline.SourceID {
	return pipeline.SourceID(index<<16 + int(partition))
}

func disassembleSourceID(sourceID pipeline.SourceID) (index int, partition int32) {
	index = int(sourceID >> 16)
	partition = int32(sourceID & 0xFFFF)

	return
}

func assembleOffset(message *kgo.Record) int64 {
	return message.Offset<<16 + int64(message.LeaderEpoch)
}

func disassembleOffset(assembledOffset int64) kgo.EpochOffset {
	offset := assembledOffset >> 16
	epoch := int32(assembledOffset & 0xFFFF)

	return kgo.EpochOffset{
		Offset: offset + 1,
		Epoch:  epoch,
	}
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}

type metaInformation struct {
	topic     string
	partition int32
	offset    int64
}

func newMetaInformation(message *kgo.Record) metaInformation {
	return metaInformation{
		topic:     message.Topic,
		partition: message.Partition,
		offset:    message.Offset,
	}
}

func (m metaInformation) GetData() map[string]any {
	return map[string]any{
		"topic":     m.topic,
		"partition": m.partition,
		"offset":    m.offset,
	}
}
