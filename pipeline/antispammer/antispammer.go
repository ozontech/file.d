package antispammer

import (
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	prom "github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type Antispammer struct {
	metricsController *metric.Ctl
	unbanIterations   int
	threshold         int
	mu                *sync.RWMutex
	sources           map[uint64]*source

	// antispammer metrics
	antispamActiveMetric *prom.GaugeVec
	antispamBanMetric    *prom.GaugeVec
}

type source struct {
	counter atomic.Int32
	name    string
}

func NewAntispammer(threshold int, unbanIterations int, maintenanceInterval time.Duration, metricsController *metric.Ctl) *Antispammer {
	if threshold != 0 {
		logger.Infof("antispam enabled, threshold=%d/%d sec", threshold, maintenanceInterval/time.Second)
	}
	antispamer := &Antispammer{
		threshold:         threshold,
		unbanIterations:   unbanIterations,
		sources:           make(map[uint64]*source),
		mu:                &sync.RWMutex{},
		metricsController: metricsController,
	}

	antispamer.antispamActiveMetric = metricsController.RegisterGauge("antispam_active", "Gauge indicates whether the antispam is enabled")
	// not enabled by default
	antispamer.antispamActiveMetric.WithLabelValues().Set(0)
	antispamer.antispamBanMetric = metricsController.RegisterGauge("antispam_banned", "How many times a source was banned")

	return antispamer
}

func (a *Antispammer) IsSpam(id uint64, name string, isNewSource bool) bool {
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
			src = &source{
				counter: atomic.Int32{},
				name:    name,
			}
			a.sources[id] = src
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
		a.antispamActiveMetric.WithLabelValues().Set(1)
		a.antispamBanMetric.WithLabelValues().Inc()
		logger.Warnf("antispam: source has been banned id=%d, name=%s", id, name)
	}

	return x >= int32(a.threshold)
}

func (a *Antispammer) Maintenance() {
	a.mu.Lock()

	allUnbanned := true
	for sourceID, source := range a.sources {
		x := int(source.counter.Load())

		if x == 0 {
			delete(a.sources, sourceID)
			continue
		}

		isMore := x >= a.threshold
		x -= a.threshold
		if x < 0 {
			x = 0
		}

		if isMore && x < a.threshold {
			a.antispamBanMetric.WithLabelValues().Dec()
			logger.Infof("antispam: source has been unbanned id=%d", sourceID)
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
		a.antispamActiveMetric.WithLabelValues().Set(0)
	} else {
		logger.Info("antispam: there are banned sources")
	}

	a.mu.Unlock()
}

func (a *Antispammer) Dump() string {
	out := logger.Cond(len(a.sources) == 0, logger.Header("no banned"), func() string {
		o := logger.Header("banned sources")
		a.mu.RLock()
		for s, source := range a.sources {
			value := source.counter.Load()
			if int(value) >= a.threshold {
				o += fmt.Sprintf("source_id: %d, source_name: %s, events_counter: %d\n", s, source.name, value)
			}
		}
		a.mu.RUnlock()
		return o
	})

	return out
}
