//go:build fuzz

// To run: go test -tags=fuzz -fuzz .
package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/twmb/franz-go/pkg/kgo"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap/zaptest"
)

const (
	defaultTopic = "default_topic"
	topicField   = "pipeline_kafka_topic"
)

type mockProducer struct {
	t *testing.T
}

func (m *mockProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	return nil
}

func (m *mockProducer) Close() {

}

func newEvent(t *testing.T, topicField, topicVal, key, val string) *pipeline.Event {
	root, err := insaneJSON.DecodeString(fmt.Sprintf(`{"%s":"%s", "%s":"%s"}`, topicField, topicVal, key, val))
	if err != nil {
		t.Skip() // ignore invalid input
	}
	return &pipeline.Event{
		Root: root,
		Buf:  make([]byte, 0, 1024),
	}
}

func FuzzKafka(f *testing.F) {
	f.Add(topicField, "some-topic", "key", "value")
	config := Config{
		Brokers:            nil,
		DefaultTopic:       defaultTopic,
		UseTopicField:      true,
		TopicField:         topicField,
		WorkersCount:       "",
		WorkersCount_:      0,
		BatchSize:          "",
		BatchSize_:         10,
		BatchFlushTimeout:  "",
		BatchFlushTimeout_: 0,
	}

	worker := pipeline.WorkerData(nil)
	logger := zaptest.NewLogger(f).Sugar()
	p := Plugin{
		logger:       logger,
		config:       &config,
		avgEventSize: 10,
		controller:   nil,
		client:       nil,
		batcher:      nil,
	}

	f.Fuzz(func(t *testing.T, topicField, topicVal, key, val string) {
		p.client = &mockProducer{
			t: t,
		}

		data := pipeline.NewPreparedBatch([]*pipeline.Event{
			newEvent(t, topicField, topicVal, key, val),
		})
		p.out(&worker, data)
	})
}
