package playground

import (
	"time"

	"go.uber.org/zap/zapcore"
)

type ZeroClock struct {
	start time.Time
}

func NewZeroClock(now time.Time) *ZeroClock {
	return &ZeroClock{start: now}
}

var _ zapcore.Clock = ZeroClock{}

func (z ZeroClock) Now() time.Time {
	diff := time.Since(z.start)
	return time.Time{}.Add(diff)
}

func (z ZeroClock) NewTicker(_ time.Duration) *time.Ticker {
	return new(time.Ticker)
}
