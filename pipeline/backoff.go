package pipeline

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type RetriableBatcher struct {
	outFn        RetriableBatcherOutFn
	batcher      *Batcher
	backoffOpts  BackoffOpts
	onRetryError func(err error)
}

type RetriableBatcherOutFn func(*WorkerData, *Batch) error

type BackoffOpts struct {
	MinRetention time.Duration
	Multiplier   float64
	AttemptNum   int
}

func NewRetriableBatcher(batcherOpts *BatcherOptions, batcherOutFn RetriableBatcherOutFn, opts BackoffOpts, onError func(err error)) *RetriableBatcher {
	batcherBackoff := &RetriableBatcher{
		outFn:        batcherOutFn,
		backoffOpts:  opts,
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
	exponentionalBackoff := backoff.ExponentialBackOff{
		InitialInterval:     b.backoffOpts.MinRetention,
		Multiplier:          b.backoffOpts.Multiplier,
		RandomizationFactor: 0.5,
		MaxInterval:         backoff.DefaultMaxInterval,
		MaxElapsedTime:      backoff.DefaultMaxElapsedTime,
		Stop:                backoff.Stop,
		Clock:               backoff.SystemClock,
	}
	exponentionalBackoff.Reset()

	var timer *time.Timer
	numTries := 0
	for {
		err := b.outFn(data, batch)
		if err == nil {
			return
		}
		next := exponentionalBackoff.NextBackOff()
		if next == backoff.Stop || (b.backoffOpts.AttemptNum >= 0 && numTries > b.backoffOpts.AttemptNum) {
			b.onRetryError(err)
			return
		}
		numTries++
		if timer == nil {
			timer = time.NewTimer(next)
		} else {
			timer.Reset(next)
		}
		<-timer.C
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
