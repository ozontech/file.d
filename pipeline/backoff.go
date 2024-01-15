package pipeline

import (
	"time"

	"github.com/cenkalti/backoff/v3"
	"github.com/ozontech/file.d/cfg"
)

type BatcherBackoff struct {
	outFn        BatcherOutFn
	backoff      backoff.BackOff
	onRetryError func(err error)
}

type BackoffOpts struct {
	MinRetention time.Duration
	Multiplier   float64
	AttemptNum   uint64
}

func NewBatcherBackoff(batcherOutFn BatcherOutFn, opts BackoffOpts, onRetryError func(err error)) *BatcherBackoff {
	boff := cfg.GetBackoff(opts.MinRetention, opts.Multiplier, opts.AttemptNum)
	return &BatcherBackoff{outFn: batcherOutFn, backoff: boff, onRetryError: onRetryError}
}

func (b *BatcherBackoff) Out(data *WorkerData, batch *Batch) {
	b.backoff.Reset()
	err := backoff.Retry(func() error {
		return b.outFn(data, batch)
	}, b.backoff)
	if err != nil {
		b.onRetryError(err)
	}
}
