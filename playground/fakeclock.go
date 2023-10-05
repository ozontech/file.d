package playground

import (
	"time"

	"go.uber.org/zap/zapcore"
)

// ZeroClock reports time since "start".
// Used to print relative time rather than absolute.
type ZeroClock struct {
	start time.Time
}

var _ zapcore.Clock = ZeroClock{}

func NewZeroClock(now time.Time) *ZeroClock {
	return &ZeroClock{start: now}
}

func (z ZeroClock) Now() time.Time {
	diff := time.Since(z.start)
	return time.Time{}.Add(diff)
}

func (z ZeroClock) NewTicker(_ time.Duration) *time.Ticker {
	return new(time.Ticker)
}
