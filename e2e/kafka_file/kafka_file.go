package kafka_file

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Struct for kafka-file plugin e2e test
type Config struct {
	Topic             string
	Broker            string
	FilesDir          string
	OutputNamePattern string
	Lines             int
	RetTime           string
	Partition         int
}

func (k *Config) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	k.FilesDir = t.TempDir()
	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", k.FilesDir+k.OutputNamePattern)
	output.Set("retention_interval", k.RetTime)
}

func (k *Config) Send(t *testing.T) {
	time.Sleep(10 * time.Second)
	connections := make([]*kafka.Conn, k.Partition)
	for i := 0; i < k.Partition; i++ {
		conn, err := kafka.DialLeader(context.Background(), "tcp", k.Broker, k.Topic, i)
		if err != nil {
			log.Fatal("failed to dial leader:", err)
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
				log.Fatal("failed to write messages:", err)
			}
		}
	}

	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}
	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (k *Config) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", k.FilesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, k.Lines*k.Partition, test.CountLines(t, logFilePattern))
}
