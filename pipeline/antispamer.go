package pipeline

import (
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
	counters        map[SourceID]*atomic.Int32
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

	metric.RegisterGauge(&metric.Desc{
		Name:      antispamActive,
		Subsystem: subsystemName,
		Help:      "Gauge indicates whether the antispam is enabled",
	})
	// not enabled by default
	metric.GetGauge(subsystemName, antispamActive).Set(0)

	metric.RegisterCounter(&metric.Desc{
		Name:      antispamBanCount,
		Subsystem: subsystemName,
		Help:      "How many times a source was banned",
	})

	return &antispamer{
		threshold:       threshold,
		unbanIterations: unbanIterations,
		counters:        make(map[SourceID]*atomic.Int32),
		mu:              &sync.RWMutex{},
	}
}

func (p *antispamer) isSpam(id SourceID, name string, isNewSource bool) bool {
	if p.threshold == 0 {
		return false
	}

	p.mu.RLock()
	value, has := p.counters[id]
	p.mu.RUnlock()

	if !has {
		p.mu.Lock()
		if newValue, has := p.counters[id]; has {
			value = newValue
		} else {
			value = &atomic.Int32{}
			p.counters[id] = value
		}
		p.mu.Unlock()
	}

	if isNewSource {
		value.Swap(0)
		return false
	}

	x := value.Inc()
	if x == int32(p.threshold) {
		value.Swap(int32(p.unbanIterations * p.threshold))
		metric.GetGauge(subsystemName, antispamActive).Set(1)
		metric.GetCounter(subsystemName, antispamBanCount).Inc()
		logger.Warnf("antispam: source has been banned id=%d, name=%s", id, name)
	}

	return x >= int32(p.threshold)
}

func (p *antispamer) maintenance() {
	p.mu.Lock()

	allUnbanned := true
	for source, counter := range p.counters {
		x := int(counter.Load())

		if x == 0 {
			delete(p.counters, source)
			continue
		}

		isMore := x >= p.threshold
		x -= p.threshold
		if x < 0 {
			x = 0
		}

		if isMore && x < p.threshold {
			logger.Infof("antispam: source has been unbanned id=%d", source)
		}

		if x >= p.threshold {
			allUnbanned = false
		}

		if x > p.unbanIterations*p.threshold {
			x = p.unbanIterations * p.threshold
		}

		counter.Swap(int32(x))
	}

	if allUnbanned {
		metric.GetGauge(subsystemName, antispamActive).Set(0)
	} else {
		logger.Info("antispam: there are banned sources")
	}

	p.mu.Unlock()
}
