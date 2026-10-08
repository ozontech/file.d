package kafka

import (
	"context"
	"sync"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

type partitionTopic struct {
	topic     string
	partition int32
}

type Consumer interface {
	Assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32)
	Revoked(_ context.Context, _ *kgo.Client, revoked map[string][]int32)
	Lost(_ context.Context, _ *kgo.Client, lost map[string][]int32)
}

type splitConsume struct {
	consumers              map[partitionTopic]*pconsumer
	bufferSize             int
	maxConcurrentConsumers int
	idByTopic              map[string]int
	controller             pipeline.InputPluginController
	logger                 *zap.Logger
	metaTemplater          *metadata.MetaTemplater
	commitErrorsMetric     *metric.Counter
	consumeErrorsMetric    *metric.Counter

	mu    sync.RWMutex
	owned map[partitionTopic]struct{}
}

func (s *splitConsume) Assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	s.mu.Lock()
	if s.owned == nil {
		s.owned = make(map[partitionTopic]struct{})
	}
	for topic, partitions := range assigned {
		for _, partition := range partitions {
			s.owned[partitionTopic{topic, partition}] = struct{}{}
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
			s.consumers[partitionTopic{topic, partition}] = pc
			go pc.consume()
		}
	}
	s.mu.Unlock()
}

func (s *splitConsume) Revoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	commitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	if err := cl.CommitMarkedOffsets(commitCtx); err != nil {
		s.commitErrorsMetric.Inc()
		s.logger.Error("can't commit marked offsets on revoke", zap.Error(err))
	}
	cancel()

	s.stopConsumers(revoked)
}

func (s *splitConsume) Lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	s.stopConsumers(lost)
}

func (s *splitConsume) stopConsumers(lost map[string][]int32) {
	s.mu.Lock()
	var wg sync.WaitGroup
	for topic, partitions := range lost {
		for _, partition := range partitions {
			tp := partitionTopic{topic, partition}
			pc := s.consumers[tp]
			delete(s.consumers, tp)
			delete(s.owned, tp)
			if pc == nil {
				continue
			}
			pc.logger.Info("waiting for finish of consume", zap.String("topic", pc.topic), zap.Int32("partiton", pc.partition))
			close(pc.quit)
			wg.Add(1)
			go func(pc *pconsumer) {
				defer wg.Done()
				<-pc.done
			}(pc)
		}
	}
	s.mu.Unlock()
	wg.Wait()
}

func (s *splitConsume) Owns(topic string, partition int32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.owned[partitionTopic{topic, partition}]
	return ok
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
			tp := partitionTopic{p.Topic, p.Partition}
			s.mu.RLock()
			consumer, ok := s.consumers[tp]
			s.mu.RUnlock()
			if !ok {
				s.logger.Error("consumer not ready yet", zap.String("topic", p.Topic), zap.Int32("partiton", p.Partition))
				return
			}
			select {
			case consumer.fetches <- p:
			case <-ctx.Done():
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
		var fetches kgo.FetchTopicPartition
		var ok bool
		select {
		case <-pc.quit:
			return
		case fetches, ok = <-pc.fetches:
			if !ok {
				return // Channel closed
			}
		}

		for i := range fetches.Records {
			if !pc.active() {
				return
			}
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
			select {
			case <-pc.quit:
				return
			default:
			}
			pc.controller.In(sourceID, "kafka", pipeline.NewOffsets(offset, nil), message.Value, true, metadataInfo)
		}
	}
}

func (pc *pconsumer) active() bool {
	select {
	case <-pc.quit:
		return false
	default:
		return true
	}
}
