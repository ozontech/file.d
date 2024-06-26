package kafka

import (
	"context"
	"crypto/tls"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kzap"
	"github.com/twmb/tlscfg"
	"go.uber.org/zap"
)

type KafkaClient interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
	Close()
}

func NewClient(c *Config, l *zap.SugaredLogger) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DefaultProduceTopic(c.DefaultTopic),
		kgo.WithLogger(kzap.New(l.Desugar())),
		kgo.MaxBufferedRecords(c.BatchSize_),
		kgo.ProducerBatchMaxBytes(int32(c.MaxMessageBytes_)),
		kgo.ProducerLinger(1 * time.Millisecond),
	}

	if c.SaslEnabled {
		switch c.SaslMechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: c.SaslUsername,
				Pass: c.SaslPassword,
			}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: c.SaslUsername,
				Pass: c.SaslPassword,
			}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: c.SaslUsername,
				Pass: c.SaslPassword,
			}.AsSha512Mechanism()))
		case "AWS_MSK_IAM":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: c.SaslUsername,
				SecretKey: c.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		}
		opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
	}

	if c.SslEnabled {
		tlsOpts := []tlscfg.Opt{}
		if c.CACert != "" || c.ClientCert != "" || c.ClientKey != "" {
			if c.CACert != "" {
				if _, err := os.Stat(c.CACert); err != nil {
					tlsOpts = append(tlsOpts,
						tlscfg.WithCA(
							[]byte(c.CACert), tlscfg.ForClient,
						),
					)
				} else {
					tlsOpts = append(tlsOpts,
						tlscfg.MaybeWithDiskCA(c.CACert, tlscfg.ForClient),
					)
				}
			}

			if _, err := os.Stat(c.ClientCert); err != nil {
				tlsOpts = append(tlsOpts,
					tlscfg.WithKeyPair(
						[]byte(c.ClientCert), []byte(c.ClientKey),
					),
				)
			} else {
				tlsOpts = append(tlsOpts,
					tlscfg.MaybeWithDiskKeyPair(c.ClientCert, c.ClientKey),
				)
			}
		}
		tc, err := tlscfg.New(tlsOpts...)
		if err != nil {
			l.Fatalf("unable to create tls config: %v", err)
		}
		tc.InsecureSkipVerify = c.SslSkipVerify
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

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
		l.Fatalf("can't create kafka client: %s", err.Error())
	}

	err = client.Ping(context.TODO())
	if err != nil {
		l.Fatalf("can't connect to kafka: %s", err.Error())
	}

	return client
}
