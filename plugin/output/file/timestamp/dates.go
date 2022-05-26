package timestamp

import (
	"go.uber.org/atomic"
)

type Pair struct {
	firstTimestamp *atomic.Uint64
	lastTimestamp  *atomic.Uint64
}

// New creates pair to timestamps.
func New(first, last uint64) *Pair {
	return &Pair{
		firstTimestamp: atomic.NewUint64(first),
		lastTimestamp:  atomic.NewUint64(last),
	}
}

// PrintFirstTimestamp returns first of timestamps.
func (p Pair) PrintFirstTimestamp() uint64 {
	return p.firstTimestamp.Load()
}

// PrintLastTimestamp returns last of timestamps.
func (p Pair) PrintLastTimestamp() uint64 {
	return p.lastTimestamp.Load()
}

// UpdatePair compares ts with first and last members, trying to replace them.
func (p Pair) UpdatePair(ts uint64) {
	first := p.firstTimestamp.Load()
	if first == 0 || ts < first {
		p.firstTimestamp.Store(ts)
	}
	if ts > p.lastTimestamp.Load() {
		p.lastTimestamp.Store(ts)
	}
}

// Reset returns pair values and resets.
func (p *Pair) Reset() (uint64, uint64) {
	return p.firstTimestamp.Swap(0), p.lastTimestamp.Swap(0)
}
