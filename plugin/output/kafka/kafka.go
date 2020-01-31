package kafka

import (
	"strings"
	"time"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"

	"github.com/Shopify/sarama"
)

/*{ introduction
Plugin sends event batches to the kafka brokers. It uses `sarama` lib.
}*/
type data struct {
	messages []*sarama.ProducerMessage
	outBuf   sarama.ByteEncoder
}

type Plugin struct {
	config     *Config
	avgLogSize int
	controller pipeline.OutputPluginController

	producer sarama.SyncProducer
	batcher  *pipeline.Batcher
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Comma-separated list of kafka brokers to write to.
	Brokers  string `json:"brokers" required:"true"` //*
	Brokers_ []string

	//> @3 @4 @5 @6
	//>
	//> Default topic name if nothing will be found in the event field or `should_use_topic_field` isn't set.
	DefaultTopic string `json:"default_topic" required:"true"` //*

	//> @3 @4 @5 @6
	//>
	//> If set plugin will use topic name from the event field.
	UseTopicField bool `json:"use_topic_field" default:"false"` //*

	//> @3 @4 @5 @6
	//>
	//> Which event field to use as topic name, if `should_use_topic_field` is set.
	TopicField string `json:"topic_field" default:"topic"` //*

	//> @3 @4 @5 @6
	//>
	//> How much workers will be instantiated to send batches.
	WorkersCount  fd.Expression `json:"workers_count" default:"gomaxprocs*4" parse:"expression"` //*
	WorkersCount_ int

	//> @3 @4 @5 @6
	//>
	//> Maximum quantity of events to pack into one batch.
	BatchSize  fd.Expression `json:"batch_size" default:"capacity/4" parse:"expression"` //*
	BatchSize_ int

	//> @3 @4 @5 @6
	//>
	//> After this timeout batch will be sent even if batch isn't full.
	BatchFlushTimeout  fd.Duration `json:"batch_flush_timeout" parse:"duration"` //*
	BatchFlushTimeout_ time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "kafka",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.config = config.(*Config)
	p.avgLogSize = params.PipelineSettings.AvgLogSize
	p.controller = params.Controller

	logger.Infof("starting kafka plugin batch size=%d", p.config.BatchSize)

	p.producer = p.newProducer()
	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"kafka",
		p.out,
		nil,
		p.controller,
		p.config.WorkersCount_,
		p.config.BatchSize_,
		p.config.BatchFlushTimeout_,
		0,
	)
	p.batcher.Start()
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			messages: make([]*sarama.ProducerMessage, p.config.BatchSize_, p.config.BatchSize_),
			outBuf:   make([]byte, 0, p.config.BatchSize_*p.avgLogSize),
		}
	}

	data := (*workerData).(*data)
	//handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgLogSize {
		data.outBuf = make(sarama.ByteEncoder, 0, p.config.BatchSize_*p.avgLogSize)
	}

	outBuf := data.outBuf[:0]
	start := 0
	for i, event := range batch.Events {
		outBuf, start = event.Encode(outBuf)

		topic := p.config.DefaultTopic
		if p.config.ShouldUseTopicField {
			fieldValue := event.Root.Dig(p.config.TopicField).AsString()
			if fieldValue != "" {
				topic = fieldValue
			}
		}

		if data.messages[i] == nil {
			data.messages[i] = &sarama.ProducerMessage{}
		}
		data.messages[i].Value = outBuf[start:]
		data.messages[i].Topic = topic
	}

	data.outBuf = outBuf

	err := p.producer.SendMessages(data.messages[:len(batch.Events)])
	if err != nil {
		errs := err.(sarama.ProducerErrors)
		for _, e := range errs {
			logger.Errorf("can't write batch to kafka: %s", e.Err.Error())
		}
		logger.Fatalf("kafka batch failed to deliver: %s", err.Error())
	}

}

func (p *Plugin) Stop() {
	p.batcher.Stop()
}

func (p *Plugin) newProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Flush.Messages = p.config.BatchSize_
	// kafka plugin itself cares for flush frequency, but we are using batcher so disable it
	config.Producer.Flush.Frequency = time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(p.config.Brokers_, config)
	if err != nil {
		logger.Fatalf("can't create kafka producer: %s", err.Error())
	}

	logger.Infof("kafka producer created with brokers %q", strings.Join(p.config.Brokers_, ","))
	return producer
}
