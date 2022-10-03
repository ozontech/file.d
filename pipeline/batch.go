package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"go.uber.org/atomic"
)

type Batch struct {
	Events []*Event
	// eventsSize contains total size of the Events in bytes
	eventsSize int
	seq        int64
	timeout    time.Duration
	startTime  time.Time

	// maxSizeCount max events per batch
	maxSizeCount int
	// maxSizeBytes max size of events per batch in bytes
	maxSizeBytes int
}

func newBatch(maxSizeCount int, maxSizeBytes int, timeout time.Duration) *Batch {
	if maxSizeCount < 0 {
		logger.Fatalf("why batch max count less than 0?")
	}
	if maxSizeBytes < 0 {
		logger.Fatalf("why batch size less than 0?")
	}
	if maxSizeCount == 0 && maxSizeBytes == 0 {
		logger.Fatalf("batch limits are not set")
	}

	return &Batch{
		maxSizeCount: maxSizeCount,
		maxSizeBytes: maxSizeBytes,
		timeout:      timeout,
		Events:       make([]*Event, 0, maxSizeCount),
	}
}

func (b *Batch) reset() {
	b.Events = b.Events[:0]
	b.eventsSize = 0
	b.startTime = time.Now()
}

func (b *Batch) append(e *Event) {
	b.Events = append(b.Events, e)
	b.eventsSize += e.Size
}

func (b *Batch) isReady() bool {
	l := len(b.Events)
	isFull := (b.maxSizeCount != 0 && l == b.maxSizeCount) || (b.maxSizeBytes != 0 && b.maxSizeBytes <= b.eventsSize)
	isTimeout := l > 0 && time.Since(b.startTime) > b.timeout
	return isFull || isTimeout
}

type Batcher struct {
	opts BatcherOptions
	// todo graceful shutdown with context.
	cancel context.CancelFunc

	shouldStop atomic.Bool
	batch      *Batch

	// cycle of batches: freeBatches => fullBatches, fullBatches => freeBatches
	// TODO get rid of freeBatches, fullBatches system, which prevents from graceful degradation.
	freeBatches chan *Batch
	fullBatches chan *Batch
	mu          *sync.Mutex
	seqMu       *sync.Mutex
	cond        *sync.Cond

	outSeq    int64
	commitSeq int64
}

type (
	BatcherOutFn         func(*WorkerData, *Batch)
	BatcherMaintenanceFn func(*WorkerData)

	BatcherOptions struct {
		PipelineName        string
		OutputType          string
		OutFn               BatcherOutFn
		MaintenanceFn       BatcherMaintenanceFn
		Controller          OutputPluginController
		Workers             int
		BatchSizeCount      int
		BatchSizeBytes      int
		FlushTimeout        time.Duration
		MaintenanceInterval time.Duration
	}
)

func NewBatcher(opts BatcherOptions) *Batcher {
	return &Batcher{opts: opts}
}

func (b *Batcher) Start(ctx context.Context) {
	b.mu = &sync.Mutex{}
	b.seqMu = &sync.Mutex{}
	b.cond = sync.NewCond(b.seqMu)
	ctx, b.cancel = context.WithCancel(ctx)

	b.freeBatches = make(chan *Batch, b.opts.Workers)
	b.fullBatches = make(chan *Batch, b.opts.Workers)
	for i := 0; i < b.opts.Workers; i++ {
		b.freeBatches <- newBatch(b.opts.BatchSizeCount, b.opts.BatchSizeBytes, b.opts.FlushTimeout)
		longpanic.Go(func() {
			b.work(ctx)
		})
	}

	longpanic.Go(b.heartbeat)
}

type WorkerData any

func (b *Batcher) work(ctx context.Context) {
	t := time.Now()
	events := make([]*Event, 0)
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.opts.OutFn(&data, batch)
		events = b.commitBatch(ctx, events, batch)

		shouldRunMaintenance := b.opts.MaintenanceFn != nil && b.opts.MaintenanceInterval != 0 && time.Since(t) > b.opts.MaintenanceInterval
		if shouldRunMaintenance {
			t = time.Now()
			b.opts.MaintenanceFn(&data)
		}
	}
}

func (b *Batcher) commitBatch(ctx context.Context, events []*Event, batch *Batch) []*Event {
	// we need to release batch first and then commit events
	// so lets swap local slice with batch slice to avoid data copying
	events, batch.Events = batch.Events, events

	batchSeq := batch.seq

	// lets restore the sequence of batches to make sure input will commit offsets incrementally
	b.seqMu.Lock()
	for b.commitSeq != batchSeq {
		b.cond.Wait()
	}
	b.commitSeq++

	for _, e := range events {
		b.opts.Controller.Commit(e)
	}

	b.cond.Broadcast()
	b.seqMu.Unlock()

	b.freeBatches <- batch

	return events
}

func (b *Batcher) heartbeat() {
	for {
		if b.shouldStop.Load() {
			return
		}

		b.mu.Lock()
		batch := b.getBatch()
		b.trySendBatchAndUnlock(batch)

		time.Sleep(time.Millisecond * 100)
	}
}

func (b *Batcher) Add(event *Event) {
	b.mu.Lock()

	batch := b.getBatch()
	batch.append(event)

	b.trySendBatchAndUnlock(batch)
}

// trySendBatch mu should be locked and it'll be unlocked after execution of this function
func (b *Batcher) trySendBatchAndUnlock(batch *Batch) {
	if !batch.isReady() {
		b.mu.Unlock()
		return
	}

	batch.seq = b.outSeq
	b.outSeq++
	b.batch = nil
	b.mu.Unlock()

	b.fullBatches <- batch
}

func (b *Batcher) getBatch() *Batch {
	if b.batch == nil {
		b.batch = <-b.freeBatches
		b.batch.reset()
	}
	return b.batch
}

func (b *Batcher) Stop() {
	b.shouldStop.Store(true)

	// todo add scenario without races.
	close(b.freeBatches)
	close(b.fullBatches)
}
