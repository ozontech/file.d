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
	// TODO get rid of freeBatches, fullBatches system, which prevents from graceful degradation.
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
		RegisterHistogram("batcher_out_fn_seconds", "", metric.SecondsBucketsLong)
	b.commitWaitingSeconds = b.opts.MetricCtl.
		RegisterHistogram("batcher_commit_waiting_seconds", "", metric.SecondsBucketsDetailed)
	b.workersInProgress = b.opts.MetricCtl.
		RegisterGauge("batcher_workers_in_progress", "")

	jobsDone := b.opts.MetricCtl.
		RegisterCounterVec("batcher_jobs_done_total", "", "status")
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
	events := make([]*Event, 0)
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.workersInProgress.Inc()

		now := time.Now()
		b.opts.OutFn(&data, batch)
		b.batchOutFnSeconds.Observe(time.Since(now).Seconds())

		events = b.commitBatch(events, batch)

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

func (b *Batcher) commitBatch(events []*Event, batch *Batch) []*Event {
	// we need to release batch first and then commit events
	// so lets swap local slice with batch slice to avoid data copying
	events, batch.Events = batch.Events, events

	batchSeq := batch.seq

	now := time.Now()
	// lets restore the sequence of batches to make sure input will commit offsets incrementally
	b.seqMu.Lock()
	for b.commitSeq != batchSeq {
		b.cond.Wait()
	}
	b.commitSeq++
	b.commitWaitingSeconds.Observe(time.Since(now).Seconds())

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

	// todo add scenario without races.
	close(b.freeBatches)
	close(b.fullBatches)
}
