package kafka

import (
	"context"
	"strings"

	"github.com/Shopify/sarama"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin reads events from listed kafka topics. It uses `sarama` lib.
It supports commitment mechanism, so it guaranties at least once delivery.
}*/

type Plugin struct {
	config        *Config
	session       sarama.ConsumerGroupSession
	consumerGroup sarama.ConsumerGroup
	cancel        context.CancelFunc
	context       context.Context
	controller    pipeline.InputPluginController
	idByTopic     map[string]int
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Comma-separated list of kafka brokers to read from.
	Brokers  string `json:"brokers" required:"true" parse:"list"`  //*
	Brokers_ []string

	//> @3 @4 @5 @6
	//>
	//> Comma separated list of kafka topics to read from.
	Topics        string `json:"topics" required:"true" parse:"list"` //*
	Topics_       []string

	//> @3 @4 @5 @6
	//>
	//> Name of consumer group to use.
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
	logger.Info("starting kafka input plugin")
	p.controller = params.Controller
	p.config = config.(*Config)

	p.idByTopic = make(map[string]int)
	for i, topic := range p.config.Topics_ {
		p.idByTopic[topic] = i
	}

	p.context, p.cancel = context.WithCancel(context.Background())
	p.consumerGroup = p.newConsumerGroup()
	p.controller.DisableStreams()

	go p.consume()
}

func (p *Plugin) consume() {
	logger.Infof("kafka input reading from topics: %s", strings.Join(p.config.Topics_, ","))
	for {
		err := p.consumerGroup.Consume(p.context, p.config.Topics_, p)
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
	index, partition := disassembleSourceID(event.SourceID)
	p.session.MarkOffset(p.config.Topics_[index], partition, event.Offset+1, "")
}

func (p *Plugin) newConsumerGroup() sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Version = sarama.V0_10_2_0

	consumerGroup, err := sarama.NewConsumerGroup(p.config.Brokers_, p.config.ConsumerGroup, config)
	if err != nil {
		logger.Fatalf("can't create kafka consumer: %s", err.Error())
	}

	return consumerGroup
}

func (p *Plugin) Setup(session sarama.ConsumerGroupSession) error {
	logger.Infof("kafka consumer created with brokers %q", strings.Join(p.config.Brokers_, ","))
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
		p.controller.In(sourceID, "kafka", message.Offset, message.Value)
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
