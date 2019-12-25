package kafka

import (
	"runtime"
	"strings"
	"time"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"

	"github.com/Shopify/sarama"
)

const (
	defaultFlushTimeout = time.Millisecond * 200
)

type Config struct {
	Brokers             string            `json:"brokers"`
	WorkersCount        int               `json:"workers_count"`
	BatchSize           int               `json:"batch_size"`
	DefaultTopic        string            `json:"default_topic"`
	ShouldUseTopicField bool              `json:"should_use_topic_field"`
	TopicField          string            `json:"topic_field"`
	FlushTimeout        pipeline.Duration `json:"flush_timeout"`

	brokers []string //split brokers string by comma
}

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

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
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

	p.config.brokers = strings.Split(p.config.Brokers, ",")
	if p.config.Brokers == "" || len(p.config.brokers) == 0 {
		logger.Fatalf("brokers isn't provided for kafka output")
	}

	if p.config.WorkersCount == 0 {
		p.config.WorkersCount = runtime.GOMAXPROCS(0) * 4
	}

	if p.config.BatchSize == 0 {
		p.config.BatchSize = params.PipelineSettings.Capacity / 4
	}

	if p.config.DefaultTopic == "" {
		logger.Fatalf(`"default_topic" isn't set for kafka output`)
	}

	if p.config.ShouldUseTopicField && p.config.TopicField == "" {
		logger.Fatalf(`"topic_field" isn't set for kafka output while "should_use_topic_filed=true"`)
	}

	if p.config.FlushTimeout.Duration == 0 {
		p.config.FlushTimeout.Duration = defaultFlushTimeout
	}

	logger.Infof("starting kafka plugin batch size=%d", p.config.BatchSize)

	p.producer = p.newProducer()
	p.batcher = pipeline.NewBatcher(
		params.PipelineName,
		"kafka",
		p.out,
		nil,
		p.controller,
		p.config.WorkersCount,
		p.config.BatchSize,
		p.config.FlushTimeout.Duration,
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
			messages: make([]*sarama.ProducerMessage, p.config.BatchSize, p.config.BatchSize),
			outBuf:   make([]byte, 0, p.config.BatchSize*p.avgLogSize),
		}
	}

	data := (*workerData).(*data)
	//handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize*p.avgLogSize {
		data.outBuf = make(sarama.ByteEncoder, 0, p.config.BatchSize*p.avgLogSize)
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
	config.Producer.Flush.Messages = p.config.BatchSize
	// kafka plugin itself cares for flush frequency, but we are using batcher so disable it
	config.Producer.Flush.Frequency = time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(p.config.brokers, config)
	if err != nil {
		logger.Fatalf("can't create kafka producer: %s", err.Error())
	}

	logger.Infof("kafka producer created with brokers %q", strings.Join(p.config.brokers, ","))
	return producer
}
