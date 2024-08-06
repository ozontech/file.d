package kafka

import (
	"context"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type KafkaClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

func NewClient(c *Config, l *zap.Logger) *kgo.Client {
	opts := cfg.GetKafkaClientOptions(c, l)
	opts = append(opts, []kgo.Opt{
		kgo.DefaultProduceTopic(c.DefaultTopic),
		kgo.MaxBufferedRecords(c.BatchSize_),
		kgo.ProducerBatchMaxBytes(int32(c.MaxMessageBytes_)),
		kgo.ProducerLinger(1 * time.Millisecond),
	}...)

	switch c.Compression {
	case "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	}

	switch c.Ack {
	case "no":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	case "all-isr":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	client, err := kgo.NewClient(opts...)

	if err != nil {
		l.Fatal("can't create kafka client", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = client.Ping(ctx)
	if err != nil {
		l.Fatal("can't connect to kafka", zap.Error(err))
	}

	return client
}
