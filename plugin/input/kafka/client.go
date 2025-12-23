package kafka

import (
	"context"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/xoauth"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

func NewClient(ctx context.Context, c *Config, l *zap.Logger, s Consumer, tokenSource xoauth.TokenSource) *kgo.Client {
	opts := cfg.GetKafkaClientOptions(c, l, tokenSource)
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
		kgo.OnPartitionsAssigned(s.Assigned),
		kgo.OnPartitionsRevoked(s.Lost),
		kgo.OnPartitionsLost(s.Lost),
		kgo.BlockRebalanceOnPoll(),
	}...)

	offset := kgo.NewOffset()
	switch c.Offset_ {
	case OffsetTypeOldest:
		offset = offset.AtStart()
	case OffsetTypeNewest:
		offset = offset.AtEnd()
	}
	opts = append(opts, kgo.ConsumeResetOffset(offset))

	switch c.Balancer {
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
		l.Fatal("can't create kafka client", zap.Error(err))
	}

	pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	err = client.Ping(pingCtx)
	if err != nil {
		l.Fatal("can't connect to kafka", zap.Error(err))
	}

	return client
}
