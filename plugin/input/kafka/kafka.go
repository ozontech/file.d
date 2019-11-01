package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
	Brokers string   `json:"brokers"`
	brokers []string //split brokers string by comma

	Topics        string   `json:"topics"`
	topics        []string //split topics string by comma
	ConsumerGroup string   `json:"consumer_group"`
	consumerGroup string
}

type Plugin struct {
	config        *Config
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	context       context.Context
	controller    pipeline.InputPluginController
	idByTopic     map[string]int
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "kafka",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
	p.config = config.(*Config)

	p.config.brokers = strings.Split(p.config.Brokers, ",")
	if p.config.Brokers == "" || len(p.config.brokers) == 0 {
		logger.Fatalf("brokers isn't provided for kafka input")
	}

	p.config.topics = strings.Split(p.config.Topics, ",")
	if p.config.Topics == "" || len(p.config.topics) == 0 {
		logger.Fatalf(`"topics" isn't set for kafka input`)
	}

	p.idByTopic = make(map[string]int)
	for i, topic := range p.config.topics {
		p.idByTopic[topic] = i
	}

	if p.config.ConsumerGroup != "" {
		p.config.consumerGroup = p.config.ConsumerGroup
	} else {
		p.config.consumerGroup = "file.d"
	}

	p.context, p.cancel = context.WithCancel(context.Background())
	p.consumerGroup = p.newConsumerGroup()
	go p.consume()
}

func (p *Plugin) consume() {
	for {
		err := p.consumerGroup.Consume(p.context, p.config.topics, p)
		if err != nil {
			logger.Errorf("can't consume from kafka: %s", err.Error())
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
		logger.Errorf("no kafka consumer session for event commit")
		return
	}
	index, partition := extractMessageParams(event.SourceID)
	p.session.MarkOffset(p.config.topics[index], partition, event.Offset+1, "")
}

func (p *Plugin) newConsumerGroup() sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V0_10_2_0

	consumerGroup, err := sarama.NewConsumerGroup(p.config.brokers, p.config.consumerGroup, config)
	if err != nil {
		logger.Fatalf("can't create kafka consumer: %s", err.Error())
	}

	return consumerGroup
}

func (p *Plugin) Setup(session sarama.ConsumerGroupSession) error {
	logger.Infof("kafka consumer created with brokers %q", strings.Join(p.config.brokers, ","))
	p.session = session
	return nil
}

func (p *Plugin) Cleanup(sarama.ConsumerGroupSession) error {
	p.session = nil
	return nil
}

func (p *Plugin) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		sourceID := makeSourceID(p.idByTopic[message.Topic], message.Partition)
		p.controller.In(sourceID, "", message.Offset, message.Value)
	}

	return nil
}

func makeSourceID(index int, partition int32) pipeline.SourceID {
	return pipeline.SourceID(index<<16 + int(partition))
}

func extractMessageParams(sourceID pipeline.SourceID) (index int, partition int32) {
	index = int(sourceID >> 16)
	partition = int32(sourceID & 0xFFFF)

	return
}
