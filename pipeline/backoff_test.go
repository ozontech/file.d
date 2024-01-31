package pipeline

import (
	"errors"
	"testing"

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

	errorFn := func(err error) {
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
		BackoffOpts{AttemptNum: 3},
		errorFn,
	)

	batcherBackoff.Out(nil, nil)

	assert.Equal(t, errorCountBefore, errorCount.Load(), "wrong error count")
	assert.Equal(t, eventCountBefore+1, eventCount.Load(), "wrong event count")
}

func TestBackoffWithError(t *testing.T) {
	errorCount := &atomic.Int32{}
	prevValue := errorCount.Load()
	errorFn := func(err error) {
		errorCount.Inc()
	}

	batcherBackoff := NewRetriableBatcher(
		&BatcherOptions{
			MetricCtl: metric.NewCtl("", prometheus.NewRegistry()),
		},
		func(workerData *WorkerData, batch *Batch) error {
			return errors.New("some error")
		},
		BackoffOpts{AttemptNum: 3},
		errorFn,
	)

	batcherBackoff.Out(nil, nil)
	assert.Equal(t, prevValue+1, errorCount.Load(), "wrong error count")
}
