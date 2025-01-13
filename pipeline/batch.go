package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
)

type BatchStatus byte

const (
	BatchStatusNotReady BatchStatus = iota
	BatchStatusMaxSizeExceeded
	BatchStatusTimeoutExceeded
)

type Batch struct {
	events []*Event
	// hasIterableEvents is the truth if the Batch contains iterable events
	hasIterableEvents bool

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

func NewPreparedBatch(events []*Event) *Batch {
	b := &Batch{}
	b.reset()
	b.events = events
	return b
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

	b := &Batch{
		maxSizeCount: maxSizeCount,
		maxSizeBytes: maxSizeBytes,
		timeout:      timeout,
		events:       make([]*Event, 0, maxSizeCount),
	}
	b.reset()

	return b
}

func (b *Batch) reset() {
	b.events = b.events[:0]
	b.eventsSize = 0
	b.status = BatchStatusNotReady
	b.hasIterableEvents = false
	b.startTime = time.Now()
}

func (b *Batch) append(e *Event) {
	b.hasIterableEvents = b.hasIterableEvents || !e.IsChildParentKind()

	b.events = append(b.events, e)
	b.eventsSize += e.Size
}

func (b *Batch) updateStatus() BatchStatus {
	l := len(b.events)
	if len(b.events) == 0 {
		// batch is empty
		return BatchStatusNotReady
	}

	switch {
	case (b.maxSizeCount != 0 && l >= b.maxSizeCount) || (b.maxSizeBytes != 0 && b.maxSizeBytes <= b.eventsSize):
		b.status = BatchStatusMaxSizeExceeded
	case l > 0 && time.Since(b.startTime) > b.timeout:
		b.status = BatchStatusTimeoutExceeded
	default:
		b.status = BatchStatusNotReady
	}
	return b.status
}

func (b *Batch) ForEach(cb func(event *Event)) {
	for _, event := range b.events {
		if event.IsChildParentKind() {
			continue
		}
		cb(event)
	}
}

type Batcher struct {
	opts BatcherOptions

	batch *Batch

	// cycle of batches: freeBatches => fullBatches, fullBatches => freeBatches
	freeBatches chan *Batch
	fullBatches chan *Batch
	workersWg   sync.WaitGroup

	mu         sync.Mutex
	shouldStop bool

	seqMu     *sync.Mutex
	cond      *sync.Cond
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
	ctl := opts.MetricCtl
	jobsDone := ctl.RegisterCounterVec("batcher_jobs_done_total", "", "status")

	freeBatches := make(chan *Batch, opts.Workers)
	fullBatches := make(chan *Batch, opts.Workers)
	for i := 0; i < opts.Workers; i++ {
		freeBatches <- newBatch(opts.BatchSizeCount, opts.BatchSizeBytes, opts.FlushTimeout)
	}

	seqMu := &sync.Mutex{}
	return &Batcher{
		seqMu:                seqMu,
		cond:                 sync.NewCond(seqMu),
		freeBatches:          freeBatches,
		fullBatches:          fullBatches,
		opts:                 opts,
		batchOutFnSeconds:    ctl.RegisterHistogram("batcher_out_fn_seconds", "", metric.SecondsBucketsLong),
		commitWaitingSeconds: ctl.RegisterHistogram("batcher_commit_waiting_seconds", "", metric.SecondsBucketsDetailed),
		workersInProgress:    ctl.RegisterGauge("batcher_workers_in_progress", ""),
		batchesDoneByMaxSize: jobsDone.WithLabelValues("max_size_exceeded"),
		batchesDoneByTimeout: jobsDone.WithLabelValues("timeout_exceeded"),
	}
}

func (b *Batcher) Start(ctx context.Context) {
	go func() {
		<-ctx.Done()
		b.Stop()
	}()

	b.workersWg.Add(b.opts.Workers)
	for i := 0; i < b.opts.Workers; i++ {
		go b.work()
	}

	go b.heartbeat()
}

type WorkerData any

func (b *Batcher) work() {
	defer b.workersWg.Done()

	t := time.Now()
	data := WorkerData(nil)
	for batch := range b.fullBatches {
		b.workersInProgress.Inc()

		if batch.hasIterableEvents {
			now := time.Now()
			b.opts.OutFn(&data, batch)
			b.batchOutFnSeconds.Observe(time.Since(now).Seconds())
		}

		status := b.commitBatch(batch)

		shouldRunMaintenance := b.opts.MaintenanceFn != nil && b.opts.MaintenanceInterval != 0 && time.Since(t) > b.opts.MaintenanceInterval
		if shouldRunMaintenance {
			t = time.Now()
			b.opts.MaintenanceFn(&data)
		}

		b.workersInProgress.Dec()
		switch status {
		case BatchStatusMaxSizeExceeded:
			b.batchesDoneByMaxSize.Inc()
		case BatchStatusTimeoutExceeded:
			b.batchesDoneByTimeout.Inc()
		default:
			logger.Panic("unreachable")
		}
	}
}

func (b *Batcher) commitBatch(batch *Batch) BatchStatus {
	batchSeq := batch.seq

	// When we sent the batch, we don’t need buffers and insaneJSON.Root.
	// We can copy the information we need and release the events to reuse them.
	// But at the moment it's hard to maintain -- plugins
	// can reuse specific Event fields in the Commit func,
	// like .Root, .streamName, .Buf

	now := time.Now()
	// let's restore the sequence of batches to make sure input will commit offsets incrementally
	b.seqMu.Lock()
	for b.commitSeq != batchSeq {
		b.cond.Wait()
	}
	b.commitSeq++
	b.commitWaitingSeconds.Observe(time.Since(now).Seconds())

	for i := range batch.events {
		b.opts.Controller.Commit(batch.events[i])
		batch.events[i] = nil
	}

	status := batch.status
	b.freeBatches <- batch
	b.cond.Broadcast()
	b.seqMu.Unlock()

	return status
}

func (b *Batcher) heartbeat() {
	for {
		b.mu.Lock()
		if b.shouldStop {
			b.mu.Unlock()
			return
		}

		batch := b.getBatch()
		b.trySendBatchAndUnlock(batch)

		time.Sleep(time.Millisecond * 100)
	}
}

func (b *Batcher) Add(event *Event) {
	b.mu.Lock()

	if b.shouldStop {
		b.mu.Unlock()
		return
	}

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
	b.mu.Lock()
	if !b.shouldStop {
		b.shouldStop = true
		close(b.fullBatches)
	}
	b.mu.Unlock()

	b.workersWg.Wait()
}
