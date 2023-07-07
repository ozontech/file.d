package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type BatchStatus byte

const (
	BatchStatusNotReady BatchStatus = iota
	BatchStatusMaxSizeExceeded
	BatchStatusTimeoutExceeded
)

type Batch struct {
	Events []*Event

	eventOffsets []*Event

	// eventsSize contains total size of the Events in bytes
	eventsSize int
	seq        int64
	timeout    time.Duration
	startTime  time.Time

	// maxSizeCount max events per batch
	maxSizeCount int
	// maxSizeBytes max size of events per batch in bytes
	maxSizeBytes int
	status       BatchStatus
}

func newBatch(maxSizeCount, maxSizeBytes int, timeout time.Duration) *Batch {
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
	b.status = BatchStatusNotReady
	b.startTime = time.Now()
}

func (b *Batch) append(e *Event) {
	b.Events = append(b.Events, e)
	b.eventsSize += e.Size
}

func (b *Batch) updateStatus() BatchStatus {
	l := len(b.Events)
	switch {
	case (b.maxSizeCount != 0 && l == b.maxSizeCount) || (b.maxSizeBytes != 0 && b.maxSizeBytes <= b.eventsSize):
		b.status = BatchStatusMaxSizeExceeded
	case l > 0 && time.Since(b.startTime) > b.timeout:
		b.status = BatchStatusTimeoutExceeded
	default:
		b.status = BatchStatusNotReady
	}
	return b.status
}

type Batcher struct {
	opts BatcherOptions

	shouldStop atomic.Bool
	batch      *Batch

	// cycle of batches: freeBatches => fullBatches, fullBatches => freeBatches
	freeBatches chan *Batch
	fullBatches chan *Batch
	mu          *sync.Mutex
	seqMu       *sync.Mutex
	cond        *sync.Cond

	outSeq    int64
	commitSeq int64

	batchOutFnSeconds    prometheus.Observer
	commitWaitingSeconds prometheus.Observer
	workersInProgress    prometheus.Gauge
	batchesDoneByMaxSize prometheus.Counter
	batchesDoneByTimeout prometheus.Counter
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
		MetricCtl           *metric.Ctl
	}
)

func NewBatcher(opts BatcherOptions) *Batcher { // nolint: gocritic // hugeParam is ok here
	return &Batcher{opts: opts}
}

// todo graceful shutdown with context.
func (b *Batcher) Start(_ context.Context) {
	b.mu = &sync.Mutex{}
	b.seqMu = &sync.Mutex{}
	b.cond = sync.NewCond(b.seqMu)
	b.batchOutFnSeconds = b.opts.MetricCtl.
		RegisterHistogram("batcher_out_fn_seconds", "", metric.SecondsBucketsLong).WithLabelValues()
	b.commitWaitingSeconds = b.opts.MetricCtl.
		RegisterHistogram("batcher_commit_waiting_seconds", "", metric.SecondsBucketsDetailed).WithLabelValues()
	b.workersInProgress = b.opts.MetricCtl.
		RegisterGauge("batcher_workers_in_progress", "").WithLabelValues()

	jobsDone := b.opts.MetricCtl.
		RegisterCounter("batcher_jobs_done_total", "", "status")
	b.batchesDoneByMaxSize = jobsDone.WithLabelValues("max_size_exceeded")
	b.batchesDoneByTimeout = jobsDone.WithLabelValues("timeout_exceeded")

	b.freeBatches = make(chan *Batch, b.opts.Workers)
	b.fullBatches = make(chan *Batch, b.opts.Workers)
	for i := 0; i < b.opts.Workers; i++ {
		b.freeBatches <- newBatch(b.opts.BatchSizeCount, b.opts.BatchSizeBytes, b.opts.FlushTimeout)
		longpanic.Go(func() {
			b.work()
		})
	}

	longpanic.Go(b.heartbeat)
}

type WorkerData any

func (b *Batcher) work() {
	t := time.Now()
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.workersInProgress.Inc()

		now := time.Now()
		b.opts.OutFn(&data, batch)
		b.batchOutFnSeconds.Observe(time.Since(now).Seconds())

		b.commitBatch(batch)

		shouldRunMaintenance := b.opts.MaintenanceFn != nil && b.opts.MaintenanceInterval != 0 && time.Since(t) > b.opts.MaintenanceInterval
		if shouldRunMaintenance {
			t = time.Now()
			b.opts.MaintenanceFn(&data)
		}

		b.workersInProgress.Dec()
		switch batch.status {
		case BatchStatusMaxSizeExceeded:
			b.batchesDoneByMaxSize.Inc()
		case BatchStatusTimeoutExceeded:
			b.batchesDoneByTimeout.Inc()
		default:
			logger.Panic("unreachable")
		}
	}
}

func (b *Batcher) commitBatch(batch *Batch) {
	batchSeq := batch.seq

	// we sent a batch, so we don’t need buffers and insaneJSON.Root,
	// so we can only copy the information we need and release the events
	events := batch.copyEventOffsets(batch.Events)
	b.opts.Controller.ReleaseEvents(batch.Events)

	now := time.Now()
	// let's restore the sequence of batches to make sure input will commit offsets incrementally
	b.seqMu.Lock()
	for b.commitSeq != batchSeq {
		b.cond.Wait()
	}
	b.commitSeq++
	b.commitWaitingSeconds.Observe(time.Since(now).Seconds())

	for i := range events {
		b.opts.Controller.Commit(events[i], false)
	}

	b.freeBatches <- batch
	b.seqMu.Unlock()

	b.cond.Broadcast()
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

// trySendBatch mu should be locked, and it'll be unlocked after execution of this function
func (b *Batcher) trySendBatchAndUnlock(batch *Batch) {
	if batch.updateStatus() == BatchStatusNotReady {
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

	b.seqMu.Lock()
	defer b.seqMu.Unlock()

	close(b.freeBatches)
	close(b.fullBatches)
}

// copyEventOffsets copies events without Root and other reusable buffers.
func (b *Batch) copyEventOffsets(events []*Event) []*Event {
	if len(b.eventOffsets) < len(events) {
		b.eventOffsets = make([]*Event, len(events))
		prealloc := make([]Event, len(events)) // store the events nearly to be more cache friendly
		for i := range b.eventOffsets {
			b.eventOffsets[i] = &prealloc[i]
		}
	}

	for i := range events {
		events[i].CopyTo(b.eventOffsets[i])
		b.eventOffsets[i].Buf = nil
		b.eventOffsets[i].Root = nil
	}

	return b.eventOffsets[:len(events)]
}
