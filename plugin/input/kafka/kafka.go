package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.
}*/

const (
	subsystemName = "input_kafka"

	commitErrors  = "commit_errors"
	consumeErrors = "consume_errors"
)

type Plugin struct {
	config        *Config
	logger        *zap.SugaredLogger
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	controller    pipeline.InputPluginController
	idByTopic     map[string]int
}

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
	// > The number of unprocessed messages in the buffer that are loaded in the background from kafka.
	ChannelBufferSize int `json:"channel_buffer_size" default:"256"` // *
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

	p.registerPluginMetrics()
	p.idByTopic = make(map[string]int, len(p.config.Topics))
	for i, topic := range p.config.Topics {
		p.idByTopic[topic] = i
	}

	ctx, cancel := context.WithCancel(context.Background())
	p.cancel = cancel
	p.consumerGroup = p.newConsumerGroup()
	p.controller.UseSpread()
	p.controller.DisableStreams()

	longpanic.Go(func() {
		p.consume(ctx)
	})
}

func (p *Plugin) registerPluginMetrics() {
	metric.RegisterCounter(&metric.MetricDesc{
		Subsystem: subsystemName,
		Name:      commitErrors,
		Help:      "Number of kafka commit errors",
	})

	metric.RegisterCounter(&metric.MetricDesc{
		Subsystem: subsystemName,
		Name:      consumeErrors,
		Help:      "Number of kafka consume errors",
	})
}

func (p *Plugin) consume(ctx context.Context) {
	p.logger.Infof("kafka input reading from topics: %s", strings.Join(p.config.Topics, ","))
	for {
		err := p.consumerGroup.Consume(ctx, p.config.Topics, p)
		if err != nil {
			metric.GetCounter(subsystemName, consumeErrors).Inc()
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
	if p.session == nil {
		metric.GetCounter(subsystemName, commitErrors).Inc()
		p.logger.Errorf("no kafka consumer session for event commit")
		return
	}
	index, partition := disassembleSourceID(event.SourceID)
	p.session.MarkOffset(p.config.Topics[index], partition, event.Offset+1, "")
}

func (p *Plugin) newConsumerGroup() sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V0_10_2_0
	config.ChannelBufferSize = p.config.ChannelBufferSize

	consumerGroup, err := sarama.NewConsumerGroup(p.config.Brokers, p.config.ConsumerGroup, config)
	if err != nil {
		p.logger.Fatalf("can't create kafka consumer: %s", err.Error())
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
		_ = p.controller.In(sourceID, "kafka", message.Offset, message.Value, true)
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
func (p *Plugin) PassEvent(event *pipeline.Event) bool {
	return true
}
