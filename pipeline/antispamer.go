package pipeline

import (
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"go.uber.org/atomic"
)

type antispamer struct {
	unbanIterations int
	threshold       int
	mu              *sync.RWMutex
	sources         map[SourceID]*source
}

type source struct {
	counter atomic.Int32
	name    string
}

const (
	subsystemName    = "antispam"
	antispamActive   = "active"
	antispamBanCount = "ban_count"
)

func newAntispamer(threshold int, unbanIterations int, maintenanceInterval time.Duration) *antispamer {
	if threshold != 0 {
		logger.Infof("antispam enabled, threshold=%d/%d sec", threshold, maintenanceInterval/time.Second)
	}

	metric.RegisterGauge(&metric.MetricDesc{
		Name:      antispamActive,
		Subsystem: subsystemName,
		Help:      "Gauge indicates whether the antispam is enabled",
	})
	// not enabled by default
	metric.GetGauge(subsystemName, antispamActive).Set(0)

	metric.RegisterCounter(&metric.MetricDesc{
		Name:      antispamBanCount,
		Subsystem: subsystemName,
		Help:      "How many times a source was banned",
	})

	return &antispamer{
		threshold:       threshold,
		unbanIterations: unbanIterations,
		sources:         make(map[SourceID]*source),
		mu:              &sync.RWMutex{},
	}
}

func (a *antispamer) isSpam(id SourceID, name string, isNewSource bool) bool {
	if a.threshold == 0 {
		return false
	}

	a.mu.RLock()
	src, has := a.sources[id]
	a.mu.RUnlock()

	if !has {
		a.mu.Lock()
		if newSrc, has := a.sources[id]; has {
			src = newSrc
		} else {
			a.sources[id] = &source{
				counter: atomic.Int32{},
				name:    name,
			}
		}
		a.mu.Unlock()
	}

	if isNewSource {
		src.counter.Swap(0)
		return false
	}

	x := src.counter.Inc()
	if x == int32(a.threshold) {
		src.counter.Swap(int32(a.unbanIterations * a.threshold))
		metric.GetGauge(subsystemName, antispamActive).Set(1)
		metric.GetCounter(subsystemName, antispamBanCount).Inc()
		logger.Warnf("antispam: source has been banned id=%d, name=%s", id, name)
	}

	return x >= int32(a.threshold)
}

func (a *antispamer) maintenance() {
	a.mu.Lock()

	allUnbanned := true
	for sourceId, source := range a.sources {
		x := int(source.counter.Load())

		if x == 0 {
			delete(a.sources, sourceId)
			continue
		}

		isMore := x >= a.threshold
		x -= a.threshold
		if x < 0 {
			x = 0
		}

		if isMore && x < a.threshold {
			logger.Infof("antispam: source has been unbanned id=%d", sourceId)
		}

		if x >= a.threshold {
			allUnbanned = false
		}

		if x > a.unbanIterations*a.threshold {
			x = a.unbanIterations * a.threshold
		}

		source.counter.Swap(int32(x))
	}

	if allUnbanned {
		metric.GetGauge(subsystemName, antispamActive).Set(0)
	} else {
		logger.Info("antispam: there are banned sources")
	}

	a.mu.Unlock()
}

func (a *antispamer) dump() string {
	out := logger.Cond(len(a.sources) == 0, logger.Header("no banned"), func() string {
		o := logger.Header("banned sources")
		a.mu.RLock()
		for s, count := range a.sources {
			o += fmt.Sprintf("source_id: %d, source_name: %s, events_counter: %d\n", s, count.name, count.counter)
		}
		a.mu.RUnlock()
		return o
	})

	return out
}
