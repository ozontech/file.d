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
	"github.com/ozontech/file.d/tls"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
It sends the event batches to kafka brokers using `sarama` lib.
}*/

const (
	outPluginType = "kafka"
)

type data struct {
	messages []*sarama.ProducerMessage
	outBuf   sarama.ByteEncoder
}

type Plugin struct {
	logger       *zap.SugaredLogger
	config       *Config
	avgEventSize int
	controller   pipeline.OutputPluginController

	producer sarama.SyncProducer
	batcher  *pipeline.Batcher

	// plugin metrics

	sendErrorMetric *prometheus.CounterVec
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
	// > If set, the plugin will use SASL authentications mechanism.
	SaslEnabled bool `json:"is_sasl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SASL authentications mechanism.
	SaslUsername string `json:"sasl_username" default:"user"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SASL authentications mechanism.
	SaslPassword string `json:"sasl_password" default:"password"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use SSL connections method.
	SaslSslEnabled bool `json:"is_ssl_enabled" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will use skip SSL verification.
	SaslSslSkipVerify bool `json:"ssl_skip_verify" default:"false"` // *

	// > @3@4@5@6
	// >
	// > If SaslSslEnabled, the plugin will use path to the PEM certificate.
	SaslPem string `json:"pem_file" default:"/file.d/certs"` // *

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
	p.logger = params.Logger
	p.avgEventSize = params.PipelineSettings.AvgEventSize
	p.controller = params.Controller

	p.logger.Infof("workers count=%d, batch size=%d", p.config.WorkersCount_, p.config.BatchSize_)

	p.producer = p.newProducer()
	p.batcher = pipeline.NewBatcher(pipeline.BatcherOptions{
		PipelineName:   params.PipelineName,
		OutputType:     outPluginType,
		OutFn:          p.out,
		Controller:     p.controller,
		Workers:        p.config.WorkersCount_,
		BatchSizeCount: p.config.BatchSize_,
		BatchSizeBytes: p.config.BatchSizeBytes_,
		FlushTimeout:   p.config.BatchFlushTimeout_,
	})

	p.batcher.Start(context.TODO())
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.batcher.Add(event)
}

func (p *Plugin) RegisterMetrics(ctl *metric.Ctl) {
	p.sendErrorMetric = ctl.RegisterCounter("output_kafka_send_errors", "Total Kafka send errors")
}

func (p *Plugin) out(workerData *pipeline.WorkerData, batch *pipeline.Batch) {
	if *workerData == nil {
		*workerData = &data{
			messages: make([]*sarama.ProducerMessage, p.config.BatchSize_),
			outBuf:   make([]byte, 0, p.config.BatchSize_*p.avgEventSize),
		}
	}

	data := (*workerData).(*data)
	// handle to much memory consumption
	if cap(data.outBuf) > p.config.BatchSize_*p.avgEventSize {
		data.outBuf = make(sarama.ByteEncoder, 0, p.config.BatchSize_*p.avgEventSize)
	}

	outBuf := data.outBuf[:0]
	start := 0
	for i, event := range batch.Events {
		outBuf, start = event.Encode(outBuf)

		topic := p.config.DefaultTopic
		if p.config.UseTopicField {
			fieldValue := event.Root.Dig(p.config.TopicField).AsString()
			if fieldValue != "" {
				topic = pipeline.CloneString(fieldValue)
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
			p.logger.Errorf("can't write batch: %s", e.Err.Error())
		}
		p.sendErrorMetric.WithLabelValues().Add(float64(len(errs)))
		p.controller.Error("some events from batch were not written")
	}
}

func (p *Plugin) Stop() {
	p.batcher.Stop()
	if err := p.producer.Close(); err != nil {
		p.logger.Error("can't stop kafka producer: %s", err)
	}
}

func (p *Plugin) newProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.ClientID = "sasl_scram_client"
	// kafka auth sasl
	if p.config.SaslEnabled {
		config.Net.SASL.Enable = true
		config.Net.SASL.Handshake = true
		config.Net.SASL.User = p.config.SaslUsername
		config.Net.SASL.Password = p.config.SaslPassword
		config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
		config.Net.SASL.Mechanism = sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512)
	}

	// kafka connect via SSL with PEM
	if p.config.SaslSslEnabled {
		config.Net.TLS.Enable = true

		tlsCfg := tls.NewConfigBuilder()
		if err := tlsCfg.AppendCARoot(p.config.SaslPem); err != nil {
			p.logger.Fatalf("can't load cert: %s", err.Error())
		}
		config.Net.TLS.Config = tlsCfg.Build()
	}

	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Flush.Messages = p.config.BatchSize_
	// kafka plugin itself cares for flush frequency, but we are using batcher so disable it.
	config.Producer.Flush.Frequency = time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(p.config.Brokers, config)
	if err != nil {
		p.logger.Fatalf("can't create producer: %s", err.Error())
	}

	p.logger.Infof("producer created with brokers %q", strings.Join(p.config.Brokers, ","))
	return producer
}
