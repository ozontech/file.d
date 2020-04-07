package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.
}*/ 

type Plugin struct {
	config        *Config
	logger        *zap.SugaredLogger
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	context       context.Context
	controller    pipeline.InputPluginController
	idByTopic     map[string]int
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The name of kafka brokers to read from.
	Brokers []string `json:"brokers" required:"true"` //*

	//> @3@4@5@6
	//>
	//> The list of kafka topics to read from.
	Topics []string `json:"topics" required:"true"` //*

	//> @3@4@5@6
	//>
	//> The name of consumer group to use.
	ConsumerGroup string `json:"consumer_group" default:"file-d"` //*
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

	p.idByTopic = make(map[string]int)
	for i, topic := range p.config.Topics {
		p.idByTopic[topic] = i
	}

	p.context, p.cancel = context.WithCancel(context.Background())
	p.consumerGroup = p.newConsumerGroup()
	p.controller.DisableStreams()

	go p.consume()
}

func (p *Plugin) consume() {
	p.logger.Infof("kafka input reading from topics: %s", strings.Join(p.config.Topics, ","))
	for {
		err := p.consumerGroup.Consume(p.context, p.config.Topics, p)
		if err != nil {
			p.logger.Errorf("can't consume from kafka: %s", err.Error())
		}

		if p.context.Err() != nil {
			return
		}
	}
}
func (p *Plugin) Stop() {
	p.cancel()
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.session == nil {
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
		p.controller.In(sourceID, "kafka", message.Offset, message.Value, true)
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
