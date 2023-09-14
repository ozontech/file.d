package split_join

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
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
	consumer sarama.ConsumerGroup
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

	addrs := []string{brokerHost}
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	admin, err := sarama.NewClusterAdmin(addrs, config)
	r.NoError(err)
	r.NoError(admin.CreateTopic(c.topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false))

	c.consumer, err = sarama.NewConsumerGroup(addrs, group, config)
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

	strBuilder := strings.Builder{}
	gotEvents := 0
	wg := sync.WaitGroup{}
	wg.Add(expectedEventsCount)

	go func() {
		r.NoError(c.consumer.Consume(ctx, []string{c.topic}, handlerFunc(func(msg *sarama.ConsumerMessage) {
			fmt.Println("consumed message", string(msg.Value), msg.Offset, msg.Partition)
			strBuilder.Write(msg.Value)
			strBuilder.WriteString("\n")
			gotEvents++
			wg.Done()
		})))
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		r.Failf("test timed out", "got: %v, expected: %v", gotEvents, expectedEventsCount)
	}

	got := strBuilder.String()

	expected := strings.Repeat(`{"first":"1"}
{"message":"start continue"}
{"second":"2"}
{"third":"3"}
`,
		messages)

	r.Equal(expected, got)
	r.Equal(expectedEventsCount, gotEvents)
}
