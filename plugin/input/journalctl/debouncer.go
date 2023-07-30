package journalctl

import (
	"sync"
	"time"
)

type Debouncer struct {
	lastCall time.Time
	// interval of time during which only 1 Do can be called
	interval time.Duration
	mx       sync.Mutex
}

func NewDebouncer(interval time.Duration) *Debouncer {
	return &Debouncer{interval: interval}
}

func (d *Debouncer) Do(cb func()) {
	d.mx.Lock()
	defer d.mx.Unlock()

	if time.Since(d.lastCall) > d.interval {
		cb()
		d.lastCall = time.Now()
	}
}
