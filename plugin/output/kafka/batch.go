package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type batch struct {
	events    []*pipeline.Event
	messages  []*sarama.ProducerMessage
	seq       int64
	size      int
	startTime time.Time
}

func newBatch(size int) *batch {
	if size <= 0 {
		logger.Fatalf("why batch size is 0?")
	}

	return &batch{
		size:     size,
		events:   make([]*pipeline.Event, 0, size),
		messages: make([]*sarama.ProducerMessage, size, size),
	}
}

func (b *batch) reset() {
	b.events = b.events[:0]
	b.startTime = time.Now()
}

func (b *batch) append(e *pipeline.Event) {
	b.events = append(b.events, e)
}

func (b *batch) isReady() bool {
	return len(b.events) == b.size || (len(b.events) > 0 && time.Now().Sub(b.startTime) > time.Second)
}
