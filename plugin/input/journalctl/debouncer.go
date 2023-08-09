package journalctl

import (
	"time"
)

type Debouncer struct {
	lastCall time.Time
	// interval of time during which only 1 Do can be called
	interval time.Duration
}

func NewDebouncer(interval time.Duration) Debouncer {
	return Debouncer{interval: interval}
}

func (d *Debouncer) Do(cb func()) {
	if d.Ready() {
		cb()
		d.lastCall = time.Now()
	}
}

func (d *Debouncer) Ready() bool {
	return time.Since(d.lastCall) > d.interval
}
