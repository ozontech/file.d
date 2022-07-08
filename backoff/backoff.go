package backoff

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	ExpBackoffDefaultMultiplier = 1.2
	ExpBackoffDefaultRndFactor  = 0.25
)

// BackOff is a wrapper that provides retry mechanism.
type BackOff struct {
	counter prom.Counter
	backoff backoff.BackOff
	timeout time.Duration
}

// RetriesCfg desribes retries count.
type RetriesCfg struct {
	Limited bool
	Limit   uint64
}

// New instance of Backoff.
func New(
	ctx context.Context,
	counter prom.Counter,
	timeout time.Duration,
	retriesCfg RetriesCfg,
	opts ...Option,
) *BackOff {
	var expBackoff backoff.BackOff
	expBackoff = backoff.WithContext(new(opts...), ctx)
	if retriesCfg.Limited {
		expBackoff = backoff.WithMaxRetries(expBackoff, retriesCfg.Limit)
	}

	return &BackOff{
		counter: counter,
		backoff: expBackoff,
		timeout: timeout,
	}
}

// RetryWithMetrics processes given lambda and increments error metric on fail.
func (b *BackOff) RetryWithMetrics(ctx context.Context, executor func(ctx context.Context) error) error {
	err := backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, b.timeout)
		defer cancel()

		if execErr := executor(ctx); execErr != nil {
			b.counter.Inc()
			return execErr
		}
		return nil
	}, b.backoff)
	return err
}

// New returns exponential backoff.
func new(opts ...Option) *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	for _, opt := range opts {
		opt(backoff)
	}
	return backoff
}

// Option confugures backoff.
type Option func(*backoff.ExponentialBackOff)

// InitialIntervalOpt set interval.
func InitialIntervalOpt(initInterval time.Duration) Option {
	return func(expBackoff *backoff.ExponentialBackOff) {
		expBackoff.InitialInterval = initInterval
	}
}

// RandomizationFactor set rand factor.
func RandomizationFactor(factor float64) Option {
	return func(expBackoff *backoff.ExponentialBackOff) {
		expBackoff.RandomizationFactor = factor
	}
}

// Multiplier sets mult.
func Multiplier(multiplier float64) Option {
	return func(expBackoff *backoff.ExponentialBackOff) {
		expBackoff.Multiplier = multiplier
	}
}

// MaxInterval set max interval.
func MaxInterval(maxInterval time.Duration) Option {
	return func(expBackoff *backoff.ExponentialBackOff) {
		expBackoff.MaxInterval = maxInterval
	}
}
