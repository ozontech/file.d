package kafka

import (
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kzap"
	"github.com/twmb/tlscfg"
	"go.uber.org/zap"
)

func newClient(c *Config, l *zap.SugaredLogger) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ConsumerGroup(c.ConsumerGroup),
		kgo.ConsumeTopics(c.Topics...),
		kgo.ClientID(c.ClientID),
		kgo.FetchMaxWait(c.ConsumerMaxWaitTime_),
		kgo.AutoCommitMarks(),
		kgo.WithLogger(kzap.New(l.Desugar())),
		kgo.MaxConcurrentFetches(c.MaxConcurrentFetches),
		kgo.FetchMaxBytes(c.FetchMaxBytes_),
		kgo.FetchMinBytes(c.FetchMinBytes_),
		kgo.AutoCommitInterval(c.AutoCommitInterval_),
		kgo.SessionTimeout(c.SessionTimeout_),
		kgo.HeartbeatInterval(c.HeartbeatInterval_),

		// kgo.RequestTimeoutOverhead(), TODO: ConsumerMaxProcessingTime?
		// https://github.com/twmb/franz-go/blob/40589af736a73742a24db882c0088669ca9cf0ca/examples/bench/main.go#L191
		// https://github.com/twmb/franz-go/blob/40589af736a73742a24db882c0088669ca9cf0ca/pkg/kgo/config.go#L132-L186
		// https://github.com/twmb/franz-go/blob/master/docs/producing-and-consuming.md#consuming
	}

	offset := kgo.NewOffset()
	switch c.Offset_ {
	case OffsetTypeOldest:
		offset = offset.AtStart()
	case OffsetTypeNewest:
		offset = offset.AtEnd()
	}
	opts = append(opts, kgo.ConsumeResetOffset(offset))

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
	}

	if c.CACert != "" || c.ClientCert != "" || c.ClientKey != "" {
		tlsOpts := []tlscfg.Opt{}

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

		tc, err := tlscfg.New(tlsOpts...)
		if err != nil {
			l.Fatalf("unable to create tls config: %v", err)
		}
		tc.InsecureSkipVerify = c.SslSkipVerify
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

	switch c.BalancerPlan {
	case "round-robin":
		opts = append(opts, kgo.Balancers(kgo.RoundRobinBalancer()))
	case "range":
		opts = append(opts, kgo.Balancers(kgo.RangeBalancer()))
	case "sticky":
		opts = append(opts, kgo.Balancers(kgo.StickyBalancer()))
	case "cooperative-sticky":
		opts = append(opts, kgo.Balancers(kgo.CooperativeStickyBalancer()))
	}

	client, err := kgo.NewClient(opts...)

	if err != nil {
		l.Fatalf("can't create kafka client: %s", err.Error())
	}

	return client
}
