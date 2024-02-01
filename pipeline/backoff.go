package pipeline

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type RetriableBatcher struct {
	outFn        RetriableBatcherOutFn
	backoff      backoff.BackOff
	batcher      *Batcher
	onRetryError func(err error)
}

type RetriableBatcherOutFn func(*WorkerData, *Batch) error

type BackoffOpts struct {
	MinRetention time.Duration
	Multiplier   float64
	AttemptNum   uint64
}

func NewRetriableBatcher(batcherOpts *BatcherOptions, batcherOutFn RetriableBatcherOutFn, opts BackoffOpts, onError func(err error)) *RetriableBatcher {
	boff := GetBackoff(
		opts.MinRetention,
		opts.Multiplier,
		opts.AttemptNum,
	)

	batcherBackoff := &RetriableBatcher{
		outFn:        batcherOutFn,
		backoff:      boff,
		onRetryError: onError,
	}
	batcherBackoff.setBatcher(batcherOpts)
	return batcherBackoff
}

func (b *RetriableBatcher) setBatcher(batcherOpts *BatcherOptions) {
	batcherOpts.OutFn = b.Out
	b.batcher = NewBatcher(*batcherOpts)
}

func (b *RetriableBatcher) Out(data *WorkerData, batch *Batch) {
	b.backoff.Reset()

	err := backoff.Retry(func() error {
		return b.outFn(data, batch)
	}, b.backoff)

	if err != nil {
		b.onRetryError(err)
	}
}

func (b *RetriableBatcher) Start(ctx context.Context) {
	b.batcher.Start(ctx)
}

func (b *RetriableBatcher) Stop() {
	b.batcher.Stop()
}

func (b *RetriableBatcher) Add(event *Event) {
	b.batcher.Add(event)
}

func GetBackoff(minRetention time.Duration, multiplier float64, attemptNum uint64) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = minRetention
	expBackoff.Multiplier = multiplier
	expBackoff.RandomizationFactor = 0.5
	return backoff.WithMaxRetries(expBackoff, attemptNum)
}
