package kafka_file

import (
	"context"
	"log"
	"path"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	kafka_out "github.com/ozontech/file.d/plugin/output/kafka"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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
	config := &kafka_out.Config{
		Brokers:          c.Brokers,
		MaxMessageBytes_: 512,
		BatchSize_:       c.Count,
	}

	client := kafka_out.NewClient(config,
		zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)),
	)
	adminClient := kadm.NewClient(client)
	_, err := adminClient.CreateTopic(context.TODO(), 1, 1, nil, c.Topics[0])
	if err != nil {
		t.Logf("cannot create topic: %s %s", c.Topics[0], err.Error())
	}

	msgs := make([]*kgo.Record, c.Partition)
	for i := range msgs {
		msgs[i] = &kgo.Record{}
		msgs[i].Value = []byte(`{"key":"value"}`)
		msgs[i].Topic = c.Topics[0]
		msgs[i].Partition = int32(i)
	}

	for i := 0; i < c.Count; i++ {
		result := client.ProduceSync(context.TODO(), msgs...)
		err := result.FirstErr()
		if err != nil {
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
