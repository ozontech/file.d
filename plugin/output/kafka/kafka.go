package kafka

import (
	"strings"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"

	"github.com/Shopify/sarama"
)

const (
	defaultWorkers    = 16
	defaultAvgLogSize = 4096

	defaultFlushTimeout = time.Millisecond * 200
)

type Config struct {
	Brokers      string `json:"brokers"`
	brokers      []string //split brokers string by comma
	DefaultTopic string `json:"default_topic"`
	WorkersCount int    `json:"workers_count"`
	BatchSize    int    `json:"batch_size"`
}

type Plugin struct {
	config *Config
	tail   pipeline.Tail

	shouldStop bool
	batch      *batch

	// cycle of batches: freeBatches => fullBatches, fullBatches => freeBatches
	freeBatches chan *batch
	fullBatches chan *batch

	producer sarama.SyncProducer

	mu        *sync.Mutex
	cond      *sync.Cond
	outSeq    int64
	commitSeq int64
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginInfo{
		Type:    "kafka",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, capacity int, tail pipeline.Tail) {
	p.config = config.(*Config)
	p.tail = tail

	p.config.brokers = strings.Split(p.config.Brokers, ",")
	if p.config.Brokers == "" || len(p.config.brokers) == 0 {
		logger.Fatalf("brokers isn't provided for kafka output")
	}

	if p.config.WorkersCount == 0 {
		p.config.WorkersCount = defaultWorkers
	}
	if p.config.BatchSize == 0 {
		p.config.BatchSize = capacity / 4
	}

	logger.Infof("starting kafka plugin batch size=%d", p.config.BatchSize)

	p.mu = &sync.Mutex{}
	p.cond = sync.NewCond(p.mu)
	p.producer = p.newProducer()

	p.freeBatches = make(chan *batch, p.config.WorkersCount)
	p.fullBatches = make(chan *batch, p.config.WorkersCount)
	for i := 0; i < p.config.WorkersCount; i++ {
		go p.work()

		p.freeBatches <- newBatch(p.config.BatchSize, defaultFlushTimeout)
	}

	go p.heartbeat()
}

func (p *Plugin) heartbeat() {
	for {
		if p.shouldStop {
			return
		}

		p.mu.Lock()
		batch := p.getBatch()
		p.trySendBatchAndUnlock(batch)

		time.Sleep(time.Millisecond * 100)
	}
}

func (p *Plugin) Out(event *pipeline.Event) {
	p.mu.Lock()

	batch := p.getBatch()
	batch.append(event)

	p.trySendBatchAndUnlock(batch)
}

// trySendBatch mu should be locked and it'll be unlocked after execution of this function
func (p *Plugin) trySendBatchAndUnlock(batch *batch) {
	if !batch.isReady() {
		p.mu.Unlock()
		return
	}

	batch.seq = p.outSeq
	p.outSeq++
	p.batch = nil
	p.mu.Unlock()

	p.fullBatches <- batch
}

func (p *Plugin) work() {
	events := make([]*pipeline.Event, 0, 0)
	out := make([]byte, 0, 0)
	for {
		batch := <-p.fullBatches
		// output buffer is going under control, let's slow down and do some GC
		if cap(out) > p.config.BatchSize*defaultAvgLogSize {
			out = make([]byte, 0, 0)
		}
		out = out[:0]

		for i, event := range batch.events {
			if batch.messages[i] == nil {
				batch.messages[i] = &sarama.ProducerMessage{}
			}

			message := batch.messages[i]

			out, start := event.Marshal(out)
			message.Value = sarama.ByteEncoder(out[start:])

			topic := event.JSON.GetStringBytes("topic")
			if topic == nil {
				message.Topic = p.config.DefaultTopic
			} else {
				message.Topic = pipeline.ByteToString(topic)
			}
		}

		err := p.producer.SendMessages(batch.messages[:len(batch.events)])
		if err != nil {
			errs := err.(sarama.ProducerErrors)

			for _, e := range errs {
				switch e.Err {
				case sarama.ErrMessageSizeTooLarge:
					logger.Fatalf("too large message, so it have been dropped while sending to kafka")
				default:
					logger.Fatalf("can't write batch to kafka: %s", err)
				}
			}
		}

		// we need to release batch first and then commit events
		// so lets exchange local slice with batch slice to avoid data copying
		tmp := events
		events = batch.events
		batch.events = tmp

		batchSeq := batch.seq

		p.freeBatches <- batch

		// lets restore the sequence of batches to make sure input will commit offsets incrementally
		p.mu.Lock()
		for p.commitSeq != batchSeq {
			p.cond.Wait()
		}
		p.commitSeq++

		for _, e := range events {
			p.tail.Commit(e)
		}

		logger.Infof("kafka batch written count=%d", len(events))

		p.cond.Broadcast()
		p.mu.Unlock()
	}
}

func (p *Plugin) Stop() {
	p.shouldStop = true
}

func (p *Plugin) getBatch() *batch {
	if p.batch == nil {
		p.batch = <-p.freeBatches
		p.batch.reset()
	}
	return p.batch
}

func (p *Plugin) newProducer() sarama.SyncProducer {
	config := sarama.NewConfig()
	config.Net.MaxOpenRequests = 1
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Flush.Messages = p.config.BatchSize
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
