package cfg

import (
	"time"

	"github.com/cenkalti/backoff/v3"
)

func GetBackoff(minRetention time.Duration, multiplier float64, attemptNum uint64) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = minRetention
	expBackoff.Multiplier = multiplier
	expBackoff.RandomizationFactor = 0.5
	return backoff.WithMaxRetries(expBackoff, attemptNum)
}
