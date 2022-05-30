//go:build fuzz

// To run: go test -tags=fuzz -fuzz .
package kafka

import (
	"fmt"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
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

func (m *mockProducer) ensureTopic(msg *sarama.ProducerMessage) {
	val, err := msg.Value.Encode()
	require.NoError(m.t, err)
	j, err := insaneJSON.DecodeBytes(val)
	if err != nil {
		m.t.Fatalf("decoding json: %v", err)
	}
	topic := j.Dig(topicField).AsString()
	if msg.Topic != topic && (topic == "" && msg.Topic != defaultTopic) {
		m.t.Fatalf("wrong topic: %s, expecting: %s for message: %s", msg.Topic, topic, string(val))
	}
}

func (m *mockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	m.ensureTopic(msg)
	return -1, -1, nil
}

func (m *mockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		m.ensureTopic(msg)
	}
	return nil
}

func (m *mockProducer) Close() error {
	return nil
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
		producer:     nil,
		batcher:      nil,
	}

	f.Fuzz(func(t *testing.T, topicField, topicVal, key, val string) {
		p.producer = &mockProducer{
			t: t,
		}

		data := pipeline.Batch{
			Events: []*pipeline.Event{
				newEvent(t, topicField, topicVal, key, val),
			},
		}
		p.out(&worker, &data)
	})
}
