package kafka

import (
	"context"

	"github.com/ozontech/file.d/cfg"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func NewClient(c *Config, l *zap.SugaredLogger) *kgo.Client {
	opts := cfg.GetKafkaClientOptions(c, l)
	opts = append(opts, []kgo.Opt{
		kgo.ConsumerGroup(c.ConsumerGroup),
		kgo.ConsumeTopics(c.Topics...),
		kgo.FetchMaxWait(c.ConsumerMaxWaitTime_),
		kgo.AutoCommitMarks(),
		kgo.MaxConcurrentFetches(c.MaxConcurrentFetches),
		kgo.FetchMaxBytes(c.FetchMaxBytes_),
		kgo.FetchMinBytes(c.FetchMinBytes_),
		kgo.AutoCommitInterval(c.AutoCommitInterval_),
		kgo.SessionTimeout(c.SessionTimeout_),
		kgo.HeartbeatInterval(c.HeartbeatInterval_),
	}...)

	offset := kgo.NewOffset()
	switch c.Offset_ {
	case OffsetTypeOldest:
		offset = offset.AtStart()
	case OffsetTypeNewest:
		offset = offset.AtEnd()
	}
	opts = append(opts, kgo.ConsumeResetOffset(offset))

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
	err = client.Ping(context.TODO())
	if err != nil {
		l.Fatalf("can't connect to kafka: %s", err.Error())
	}

	return client
}
