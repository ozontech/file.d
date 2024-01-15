package pipeline

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestBackoff(t *testing.T) {
	errorCount := &atomic.Int32{}
	prevValue := errorCount.Load()
	errorFn := func(err error) {
		errorCount.Inc()
	}

	batcherBackoff := NewBatcherBackoff(
		func(workerData *WorkerData, batch *Batch) error {
			return nil
		},
		BackoffOpts{AttemptNum: 3},
		errorFn,
	)

	batcherBackoff.Out(nil, nil)
	assert.Equal(t, prevValue, errorCount.Load(), "wrong error count")
}

func TestBackoffWithError(t *testing.T) {
	errorCount := &atomic.Int32{}
	prevValue := errorCount.Load()
	errorFn := func(err error) {
		errorCount.Inc()
	}

	batcherBackoff := NewBatcherBackoff(
		func(workerData *WorkerData, batch *Batch) error {
			return errors.New("some error")
		},
		BackoffOpts{AttemptNum: 3},
		errorFn,
	)

	batcherBackoff.Out(nil, nil)
	assert.Equal(t, prevValue+1, errorCount.Load(), "wrong error count")
}
