package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type batcherTail struct {
	commit func(event *Event)
}

func (b *batcherTail) Commit(event *Event) {
	b.commit(event)
}

func (b *batcherTail) Error(err string) {
	logger.Panic(err)
}

func (b *batcherTail) RecoverFromPanic() {}

func TestBatcher(t *testing.T) {
	eventCount := 10000000
	batchSize := 100
	processors := 16

	wg := sync.WaitGroup{}
	wg.Add(eventCount)

	batchCount := &atomic.Int32{}
	batcherOut := func(workerData *WorkerData, batch *Batch) {
		if *workerData == nil {
			*workerData = batchCount
		}
		counter := (*workerData).(*atomic.Int32)
		counter.Inc()
	}

	seqIDs := make(map[SourceID]uint64)
	commitsCount := atomic.Int32{}
	batcherTail := &batcherTail{commit: func(event *Event) {
		if _, has := seqIDs[event.SourceID]; !has {
			seqIDs[event.SourceID] = event.SeqID
		}
		if event.SeqID < seqIDs[event.SourceID] {
			logger.Panicf("wrong batch sequence:source=%d seq=%d, prev seq=%d", event.SourceID, event.SeqID, seqIDs[event.SourceID])
		}
		seqIDs[event.SourceID] = event.SeqID

		commitsCount.Inc()
		wg.Done()
	}}

	batcher := NewBatcher(BatcherOptions{
		PipelineName:   "test",
		OutputType:     "devnull",
		OutFn:          batcherOut,
		Controller:     batcherTail,
		Workers:        8,
		BatchSizeCount: batchSize,
		FlushTimeout:   time.Second,
		MetricCtl:      metric.NewCtl("", prometheus.NewRegistry()),
	})

	ctx := context.TODO()
	batcher.Start(ctx)

	eventsCh := make(chan *Event, 1024)
	go func() {
		for i := 0; i < eventCount; i++ {
			eventsCh <- &Event{SeqID: uint64(i)}
		}
		close(eventsCh)
	}()

	for p := 0; p < processors; p++ {
		go func(x int) {
			for event := range eventsCh {
				event.SourceID = SourceID(x)
				batcher.Add(event)
			}
		}(p)
	}

	wg.Wait()
	batcher.Stop()

	assert.Equal(t, int32(eventCount), commitsCount.Load(), "wrong commits count")
	assert.Equal(t, int32(eventCount/batchSize), batchCount.Load(), "wrong batches count")
}

func TestBatcherMaxSize(t *testing.T) {
	eventCount := 10_000_000
	batchSize := 1_000_000
	eventSize := 1000
	processors := 16

	wg := sync.WaitGroup{}
	wg.Add(eventCount)

	batchCount := &atomic.Int32{}
	batcherOut := func(workerData *WorkerData, batch *Batch) {
		if *workerData == nil {
			*workerData = batchCount
		}
		counter := (*workerData).(*atomic.Int32)
		counter.Inc()
	}

	seqIDs := make(map[SourceID]uint64)
	commitsCount := atomic.Int32{}
	tail := &batcherTail{commit: func(event *Event) {
		if _, has := seqIDs[event.SourceID]; !has {
			seqIDs[event.SourceID] = event.SeqID
		}
		if event.SeqID < seqIDs[event.SourceID] {
			logger.Panicf("wrong batch sequence:source=%d seq=%d, prev seq=%d", event.SourceID, event.SeqID, seqIDs[event.SourceID])
		}
		seqIDs[event.SourceID] = event.SeqID

		commitsCount.Inc()
		wg.Done()
	}}

	batcher := NewBatcher(BatcherOptions{
		PipelineName:   "test",
		OutputType:     "devnull",
		OutFn:          batcherOut,
		Controller:     tail,
		Workers:        8,
		BatchSizeBytes: batchSize,
		FlushTimeout:   time.Minute,
		MetricCtl:      metric.NewCtl("", prometheus.NewRegistry()),
	})

	batcher.Start(context.Background())

	eventsCh := make(chan *Event, 1024)
	go func() {
		for i := 0; i < eventCount; i++ {
			eventsCh <- &Event{SeqID: uint64(i), Size: eventSize}
		}
		close(eventsCh)
	}()

	for p := 0; p < processors; p++ {
		go func(x int) {
			for event := range eventsCh {
				event.SourceID = SourceID(x)
				batcher.Add(event)
			}
		}(p)
	}

	wg.Wait()
	batcher.Stop()

	assert.Equal(t, int32(eventCount), commitsCount.Load(), "wrong commits count")
	assert.Equal(t, int32(eventCount/(batchSize/eventSize)), batchCount.Load(), "wrong batches count")
}
