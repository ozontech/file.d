package xoauth

import (
	"math"
	"math/rand"
	"time"
)

// exponentialJitterBackoff provides a callback which will
// perform en exponential backoff based on the attempt number and with jitter to
// prevent a thundering herd.
//
// min and max here are *not* absolute values. The number to be multiplied by
// the attempt number will be chosen at random from between them, thus they are
// bounding the jitter.
func exponentialJitterBackoff() func(min, max time.Duration, attemptNum int) time.Duration {
	rand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

	return func(min, max time.Duration, attemptNum int) time.Duration {
		minf := float64(min)
		mult := math.Pow(2, float64(attemptNum)) * minf

		jitter := rand.Float64() * (mult - minf)

		sleepf := mult + jitter
		maxf := float64(max)
		if sleepf > maxf {
			sleepf = maxf
		}

		return time.Duration(sleepf)
	}
}

// linearJitterBackoff will perform linear backoff based on the attempt number and with jitter to
// prevent a thundering herd.
//
// min and max here are *not* absolute values. The number to be multiplied by
// the attempt number will be chosen at random from between them, thus they are
// bounding the jitter.
//
// For instance:
// - To get strictly linear backoff of one second increasing each retry, set
// both to one second (1s, 2s, 3s, 4s, ...)
// - To get a small amount of jitter centered around one second increasing each
// retry, set to around one second, such as a min of 800ms and max of 1200ms
// (892ms, 2102ms, 2945ms, 4312ms, ...)
// - To get extreme jitter, set to a very wide spread, such as a min of 100ms
// and a max of 20s (15382ms, 292ms, 51321ms, 35234ms, ...)
func linearJitterBackoff() func(min, max time.Duration, attemptNum int) time.Duration {
	rand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))

	return func(min, max time.Duration, attemptNum int) time.Duration {
		// attemptNum always starts at zero but we want to start at 1 for multiplication
		attemptNum++

		if max <= min {
			// Unclear what to do here, or they are the same, so return min *
			// attemptNum
			return min * time.Duration(attemptNum)
		}

		// Pick a random number that lies somewhere between the min and max and
		// multiply by the attemptNum. attemptNum starts at zero so we always
		// increment here. We first get a random percentage, then apply that to the
		// difference between min and max, and add to min.
		jitter := rand.Float64() * float64(max-min)

		jitterMin := int64(jitter) + int64(min)
		return time.Duration(jitterMin * int64(attemptNum))
	}
}
