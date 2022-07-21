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

	// maxQuantity max events per batch
	maxQuantity int
	// maxSize max size of events per batch in bytes
	maxSize int
}

func newBatch(quantity int, size int, timeout time.Duration) *Batch {
	if quantity < 0 {
		logger.Fatalf("why batch quantity less than 0?")
	}
	if size < 0 {
		logger.Fatalf("why batch size less than 0?")
	}

	return &Batch{
		maxQuantity: quantity,
		maxSize:     size,
		timeout:     timeout,
		Events:      make([]*Event, 0, quantity),
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
	isFull := (b.maxQuantity != 0 && l == b.maxQuantity) || (b.maxSize != 0 && b.maxSize <= b.eventsSize)
	isTimeout := l > 0 && time.Since(b.startTime) > b.timeout
	return isFull || isTimeout
}

type Batcher struct {
	pipelineName        string
	outputType          string
	outFn               BatcherOutFn
	maintenanceFn       BatcherMaintenanceFn
	controller          OutputPluginController
	workerCount         int
	batchQuantity       int
	batchSizeBytes      int
	flushTimeout        time.Duration
	maintenanceInterval time.Duration
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
)

func NewBatcher(
	pipelineName string,
	outputType string,
	outFn BatcherOutFn,
	maintenanceFn BatcherMaintenanceFn,
	controller OutputPluginController,
	workers int,
	batchSize int,
	batchSizeBytes int,
	flushTimeout time.Duration,
	maintenanceInterval time.Duration,
) *Batcher {
	return &Batcher{
		pipelineName:        pipelineName,
		outputType:          outputType,
		outFn:               outFn,
		maintenanceFn:       maintenanceFn,
		controller:          controller,
		workerCount:         workers,
		batchQuantity:       batchSize,
		batchSizeBytes:      batchSizeBytes,
		flushTimeout:        flushTimeout,
		maintenanceInterval: maintenanceInterval,
	}
}

func (b *Batcher) Start(ctx context.Context) {
	b.mu = &sync.Mutex{}
	b.seqMu = &sync.Mutex{}
	b.cond = sync.NewCond(b.seqMu)
	ctx, b.cancel = context.WithCancel(ctx)

	b.freeBatches = make(chan *Batch, b.workerCount)
	b.fullBatches = make(chan *Batch, b.workerCount)
	for i := 0; i < b.workerCount; i++ {
		b.freeBatches <- newBatch(b.batchQuantity, b.batchSizeBytes, b.flushTimeout)
		longpanic.Go(func() {
			b.work(ctx)
		})
	}

	longpanic.Go(b.heartbeat)
}

type WorkerData interface{}

func (b *Batcher) work(ctx context.Context) {
	t := time.Now()
	events := make([]*Event, 0)
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.outFn(&data, batch)
		events = b.commitBatch(ctx, events, batch)

		shouldRunMaintenance := b.maintenanceFn != nil && b.maintenanceInterval != 0 && time.Since(t) > b.maintenanceInterval
		if shouldRunMaintenance {
			t = time.Now()
			b.maintenanceFn(&data)
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
		b.controller.Commit(e)
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
