package kafka

import (
	"context"
	"sync"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type tp struct {
	t string
	p int32
}

type Consumer interface {
	Assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32)
	Lost(_ context.Context, _ *kgo.Client, lost map[string][]int32)
}

type splitConsume struct {
	consumers              map[tp]*pconsumer
	bufferSize             int
	maxConcurrentConsumers int
	idByTopic              map[string]int
	controller             pipeline.InputPluginController
	logger                 *zap.Logger
	metaTemplater          *metadata.MetaTemplater
	consumeErrorsMetric    prometheus.Counter
}

func (s *splitConsume) Assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			pc := &pconsumer{
				topic:     topic,
				partition: partition,
				topicID:   s.idByTopic[topic],

				quit:    make(chan struct{}),
				done:    make(chan struct{}),
				fetches: make(chan kgo.FetchTopicPartition, s.maxConcurrentConsumers),

				controller:    s.controller,
				logger:        s.logger,
				metaTemplater: s.metaTemplater,
			}
			s.consumers[tp{topic, partition}] = pc
			go pc.consume()
		}
	}
}

func (s *splitConsume) Lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	var wg sync.WaitGroup
	defer wg.Wait()

	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := tp{topic, partition}
			pc := s.consumers[tp]
			delete(s.consumers, tp)
			close(pc.quit)
			pc.logger.Info("waiting for finish of consume", zap.String("topic", pc.topic), zap.Int32("partiton", pc.partition))
			wg.Add(1)
			go func(pc *pconsumer) {
				defer wg.Done()
				<-pc.done
			}(pc)
		}
	}
}

func (s *splitConsume) consume(ctx context.Context, cl *kgo.Client) {
	for {
		fetches := cl.PollRecords(ctx, s.bufferSize)
		if fetches.IsClientClosed() {
			return
		}

		if ctx.Err() != nil {
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				s.consumeErrorsMetric.Inc()
				s.logger.Error("can't consume from kafka", zap.Error(err.Err))
			}
		}

		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			tp := tp{p.Topic, p.Partition}
			if consumer, ok := s.consumers[tp]; ok {
				consumer.fetches <- p
			} else {
				s.logger.Error("consumer not ready yet", zap.String("topic", p.Topic), zap.Int32("partiton", p.Partition))
			}
		})
		cl.AllowRebalance()
	}
}

type pconsumer struct {
	topic     string
	partition int32
	topicID   int

	quit    chan struct{}
	done    chan struct{}
	fetches chan kgo.FetchTopicPartition

	controller    pipeline.InputPluginController
	logger        *zap.Logger
	metaTemplater *metadata.MetaTemplater
}

func (pc *pconsumer) consume() {
	defer close(pc.done)
	pc.logger.Info("starting consume", zap.String("topic", pc.topic), zap.Int32("partiton", pc.partition))
	defer pc.logger.Info("closing consume", zap.String("topic", pc.topic), zap.Int32("partiton", pc.partition))
	for {
		select {
		case <-pc.quit:
			return
		case fetches, ok := <-pc.fetches:
			if !ok {
				return // Channel closed
			}
			for i := range fetches.Records {
				message := fetches.Records[i]
				sourceID := assembleSourceID(
					pc.topicID,
					message.Partition,
				)

				offset := assembleOffset(message)
				var metadataInfo metadata.MetaData
				var err error
				if pc.metaTemplater != nil {
					metadataInfo, err = pc.metaTemplater.Render(newMetaInformation(message))
					if err != nil {
						pc.logger.Error("can't render meta data", zap.Error(err))
					}
				}
				_ = pc.controller.In(sourceID, "kafka", Offset(offset), message.Value, true, metadataInfo)
			}
		}
	}
}

type Offset int64

func (o Offset) Current() int64 {
	return int64(o)
}

func (o Offset) ByStream(_ string) int64 {
	panic("unimplemented")
}
