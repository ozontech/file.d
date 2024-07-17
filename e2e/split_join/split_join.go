package split_join

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	kafka_in "github.com/ozontech/file.d/plugin/input/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	brokerHost = "localhost:9092"
	group      = "file_d_test_split_join_client"

	arrayLen = 4
	sample   = `{ "data": [ { "first": "1" }, { "message": "start " }, { "message": "continue" }, { "second": "2" }, { "third": "3" } ] }`

	messages = 10
)

type Config struct {
	inputDir string
	client   *kgo.Client
	topic    string
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	r := require.New(t)

	c.inputDir = t.TempDir()
	offsetsDir := t.TempDir()
	c.topic = fmt.Sprintf("file_d_test_split_join_%d", time.Now().UnixNano())
	t.Logf("generated topic: %s", c.topic)

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.inputDir)
	input.Set("filename_pattern", "input.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("brokers", []string{brokerHost})
	output.Set("default_topic", c.topic)

	config := &kafka_in.Config{
		Brokers:              []string{brokerHost},
		Topics:               []string{c.topic},
		ConsumerGroup:        group,
		Offset_:              kafka_in.OffsetTypeOldest,
		SessionTimeout_:      10 * time.Second,
		AutoCommitInterval_:  1 * time.Second,
		ConsumerMaxWaitTime_: 1 * time.Second,
		HeartbeatInterval_:   10 * time.Second,
	}

	c.client = kafka_in.NewClient(config,
		zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)),
	)

	adminClient := kadm.NewClient(c.client)
	_, err := adminClient.CreateTopic(context.TODO(), 1, 1, nil, c.topic)
	r.NoError(err)
}

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	for i := 0; i < messages; i++ {
		_, err = file.WriteString(sample + "\n")
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	r := require.New(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	expectedEventsCount := messages * arrayLen
	result := make(map[string]int, arrayLen)
	gotEvents := 0
	done := make(chan struct{})

	go func() {
		for {
			fetches := c.client.PollFetches(ctx)
			fetches.EachError(func(topic string, p int32, err error) {})
			fetches.EachRecord(func(r *kgo.Record) {
				result[string(r.Value)]++
				gotEvents++
				if gotEvents == expectedEventsCount {
					close(done)
				}
			})
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		r.Failf("test timed out", "got: %v, expected: %v", gotEvents, expectedEventsCount)
	}

	expected := map[string]int{
		"{\"first\":\"1\"}":                messages,
		"{\"message\":\"start continue\"}": messages,
		"{\"second\":\"2\"}":               messages,
		"{\"third\":\"3\"}":                messages,
	}

	r.True(reflect.DeepEqual(expected, result))
	r.Equal(expectedEventsCount, gotEvents)
}
