package kafka_file

import (
	"log"
	"path"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In this test, a message sender is created that generates one message for each partition. These messages are sent Count times.
// They are processed by the pipeline. We wait for the end of processing and fix the number of processed messages.

// Config for kafka-file plugin e2e test
type Config struct {
	Topics    []string
	Brokers   []string
	FilesDir  string
	Count     int
	RetTime   string
	Partition int
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.FilesDir = t.TempDir()

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(c.FilesDir, "file-d.log"))
	output.Set("retention_interval", c.RetTime)

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("brokers", c.Brokers)
	input.Set("topics", c.Topics)
}

// Send creates a Partition of messages (one for each partition) and sends them Count times to kafka
func (c *Config) Send(t *testing.T) {
	config := sarama.NewConfig()
	config.Producer.Flush.Frequency = time.Millisecond
	config.Producer.Return.Errors = true
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(c.Brokers, config)
	if err != nil {
		log.Fatalf("failed to create async producer: %s", err.Error())
	}
	msgs := make([]*sarama.ProducerMessage, c.Partition)
	message := sarama.StringEncoder(`{"key":"value"}`)

	for i := range msgs {
		msgs[i] = &sarama.ProducerMessage{}
		msgs[i].Topic = c.Topics[0]
		msgs[i].Value = message
		msgs[i].Partition = int32(i)
	}

	for i := 0; i < c.Count; i++ {
		if err = producer.SendMessages(msgs); err != nil {
			log.Fatalf("failed to send messages: %s", err.Error())
		}
	}
}

// Validate waits for the message processing to complete
func (c *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(c.FilesDir, "file-d*.log")
	test.WaitProcessEvents(t, c.Count*c.Partition, 3*time.Second, 20*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "no files with processed events")
	require.Equal(t, c.Count*c.Partition, test.CountLines(t, logFilePattern), "wrong number of processed events")
}
