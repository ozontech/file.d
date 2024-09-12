package antispam

import (
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Antispammer makes a decision on the need to parse the input log.
// It can be useful when any application writes logs at speed faster than File.d can read it.
//
// Anti-spammer supports exceptions for cases where you need to guarantee delivery of an important events.
type Antispammer struct {
	unbanIterations     int
	threshold           int
	maintenanceInterval time.Duration
	mu                  sync.RWMutex
	sources             map[any]source
	exceptions          matchrule.RuleSets

	logger *zap.Logger

	// antispammer metrics
	activeMetric    prometheus.Gauge
	banMetric       metric.HeldGaugeVec
	exceptionMetric *prometheus.CounterVec
}

type source struct {
	counter   *atomic.Int32
	timestamp *atomic.Int64
	name      string
}

type Options struct {
	MaintenanceInterval time.Duration
	Threshold           int
	Field               string
	UnbanIterations     int
	Exceptions          matchrule.RuleSets

	Logger            *zap.Logger
	MetricsController *metric.Ctl
	MetricHolder      *metric.Holder
}

func NewAntispammer(o *Options) *Antispammer {
	if o.Threshold > 0 {
		o.Logger.Info("antispam enabled",
			zap.Int("threshold", o.Threshold),
			zap.Duration("maintenance", o.MaintenanceInterval))
	}

	banMetric := o.MetricsController.RegisterGaugeVec("antispam_banned", "Source is banned", "source_name")

	a := &Antispammer{
		unbanIterations:     o.UnbanIterations,
		threshold:           o.Threshold,
		maintenanceInterval: o.MaintenanceInterval,
		sources:             make(map[any]source),
		exceptions:          o.Exceptions,
		logger:              o.Logger,
		activeMetric: o.MetricsController.RegisterGauge("antispam_active",
			"Gauge indicates whether the antispam is enabled",
		),
		banMetric: o.MetricHolder.AddGaugeVec(banMetric),
		exceptionMetric: o.MetricsController.RegisterCounterVec("antispam_exceptions",
			"How many times an exception match with an event",
			"name",
		),
	}

	// not enabled by default
	a.activeMetric.Set(0)

	return a
}

func (a *Antispammer) IsSpam(id any, name string, isNewSource bool, event []byte, timeEvent time.Time) bool {
	if a.threshold <= 0 {
		return false
	}

	for i := 0; i < len(a.exceptions); i++ {
		e := &a.exceptions[i]
		if e.Match(event) {
			if e.Name != "" {
				a.exceptionMetric.WithLabelValues(e.Name).Inc()
			}
			return false
		}
	}

	a.mu.RLock()
	src, has := a.sources[id]
	a.mu.RUnlock()

	timeEventSeconds := timeEvent.UnixNano()

	if !has {
		a.mu.Lock()
		if newSrc, has := a.sources[id]; has {
			src = newSrc
		} else {
			src = source{
				counter:   &atomic.Int32{},
				name:      name,
				timestamp: &atomic.Int64{},
			}
			src.timestamp.Add(timeEventSeconds)
			a.sources[id] = src
		}
		a.mu.Unlock()
	}

	if isNewSource {
		src.counter.Swap(0)
		return false
	}

	x := src.counter.Load()
	diff := timeEventSeconds - src.timestamp.Swap(timeEventSeconds)
	if diff < a.maintenanceInterval.Nanoseconds() {
		x = src.counter.Inc()
	}
	if x == int32(a.threshold) {
		src.counter.Swap(int32(a.unbanIterations * a.threshold))
		a.activeMetric.Set(1)
		a.banMetric.WithLabelValues(name).Inc()
		a.logger.Warn("source has been banned",
			zap.Any("id", id), zap.String("name", name),
			zap.Time("time_event", timeEvent), zap.Int64("diff_nsec", diff),
			zap.Int64("maintenance_nsec", a.maintenanceInterval.Nanoseconds()),
			zap.Int32("counter", src.counter.Load()),
		)
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
			a.banMetric.WithLabelValues(source.name).Dec()
			a.logger.Info("source has been unbanned", zap.Any("id", sourceID))
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
		a.activeMetric.Set(0)
	} else {
		a.logger.Info("there are banned sources")
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
				o += fmt.Sprintf("source_id: %v, source_name: %s, counter: %d\n", s, source.name, value)
			}
		}
		a.mu.RUnlock()
		return o
	})

	return out
}
