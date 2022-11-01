package kafka_file

import (
	"context"
	"log"
	"path"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Config for kafka-file plugin e2e test
type Config struct {
	Topic     string
	Broker    string
	FilesDir  string
	Lines     int
	RetTime   string
	Partition int
}

func (k *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	k.FilesDir = t.TempDir()
	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(k.FilesDir, "file-d.log"))
	output.Set("retention_interval", k.RetTime)
}

func (k *Config) Send(t *testing.T) {
	connections := make([]*kafka.Conn, k.Partition)
	for i := 0; i < k.Partition; i++ {
		conn, err := kafka.DialLeader(context.Background(), "tcp", k.Broker, k.Topic, i)
		if err != nil {
			log.Fatalf("failed to dial leader: %s", err.Error())
		}
		conn.SetWriteDeadline(time.Now().Add(20 * time.Second))
		connections[i] = conn
	}

	for i := 0; i < k.Lines; i++ {
		for _, conn := range connections {
			_, err := conn.WriteMessages(
				kafka.Message{Value: []byte(`{"key":"value"}`)},
			)
			if err != nil {
				log.Fatalf("failed to write messages: %s", err.Error())
			}
		}
	}

	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			log.Fatalf("failed to close writer: %s", err.Error())
		}
	}
}

func (k *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(k.FilesDir, "file-d*.log")
	test.WaitProcessEvents(t, k.Lines*k.Partition, 3*time.Second, 15*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, k.Lines*k.Partition, test.CountLines(t, logFilePattern))
}
