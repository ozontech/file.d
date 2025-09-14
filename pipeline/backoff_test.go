package pipeline

import (
	"errors"
	"testing"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestBackoff(t *testing.T) {
	errorCount := &atomic.Int32{}
	errorCountBefore := errorCount.Load()

	eventCount := &atomic.Int32{}
	eventCountBefore := eventCount.Load()

	errorFn := func(err error, events []*Event) {
		errorCount.Inc()
	}

	batcherBackoff := NewRetriableBatcher(
		&BatcherOptions{
			MetricCtl: metric.NewCtl("", prometheus.NewRegistry()),
		},
		func(workerData *WorkerData, batch *Batch) error {
			eventCount.Inc()
			return nil
		},
		BackoffOpts{
			AttemptNum:           3,
			IsDeadQueueAvailable: false,
		},
		errorFn,
	)

	data := WorkerData(nil)
	batch := newBatch(1, 1, 1*time.Second)
	batch.append(&Event{})
	batcherBackoff.Out(&data, batch)
	assert.Equal(t, errorCountBefore, errorCount.Load(), "wrong error count")
	assert.Equal(t, eventCountBefore+1, eventCount.Load(), "wrong event count")
}

func TestBackoffWithError(t *testing.T) {
	errorCount := &atomic.Int32{}
	prevValue := errorCount.Load()
	errorFn := func(err error, events []*Event) {
		errorCount.Inc()
	}

	batcherBackoff := NewRetriableBatcher(
		&BatcherOptions{
			MetricCtl: metric.NewCtl("", prometheus.NewRegistry()),
		},
		func(workerData *WorkerData, batch *Batch) error {
			return errors.New("some error")
		},
		BackoffOpts{
			AttemptNum:           3,
			IsDeadQueueAvailable: false,
		},
		errorFn,
	)

	data := WorkerData(nil)
	batch := newBatch(1, 1, 1*time.Second)
	batch.append(&Event{})
	batcherBackoff.Out(&data, batch)
	assert.Equal(t, prevValue+1, errorCount.Load(), "wrong error count")
	assert.Equal(t, 1, len(batch.events), "wrong number of events in batch")
	assert.Equal(t, BatchStatusNotReady, batch.status, "wrong batch status")
}

func TestBackoffWithErrorWithDeadQueue(t *testing.T) {
	errorCount := &atomic.Int32{}
	prevValue := errorCount.Load()
	errorFn := func(err error, events []*Event) {
		errorCount.Inc()
	}

	batcherBackoff := NewRetriableBatcher(
		&BatcherOptions{
			MetricCtl: metric.NewCtl("", prometheus.NewRegistry()),
		},
		func(workerData *WorkerData, batch *Batch) error {
			return errors.New("some error")
		},
		BackoffOpts{
			AttemptNum:           3,
			IsDeadQueueAvailable: true,
		},
		errorFn,
	)

	data := WorkerData(nil)
	batch := newBatch(1, 1, 1*time.Second)
	batch.append(&Event{})
	batcherBackoff.Out(&data, batch)
	assert.Equal(t, prevValue+1, errorCount.Load(), "wrong error count")
	assert.Equal(t, 0, len(batch.events), "wrong number of events in batch")
	assert.Equal(t, BatchStatusInDeadQueue, batch.status, "wrong batch status")
}
