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

const (
	thresholdUnlimited = -1
	thresholdBlocked   = 0
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
	sources             map[string]source
	rules               Rules
	enabled             bool

	logger *zap.Logger

	// antispammer metrics
	activeMetric    prometheus.Gauge
	banMetric       *prometheus.GaugeVec
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
	UnbanIterations     int
	Exceptions          Exceptions
	ConfigV2            map[string]any

	Logger            *zap.Logger
	MetricsController *metric.Ctl
	MetricHolder      *metric.Holder
}

func NewAntispammer(o *Options) *Antispammer {
	a := &Antispammer{
		unbanIterations:     o.UnbanIterations,
		maintenanceInterval: o.MaintenanceInterval,
		sources:             make(map[string]source),
		logger:              o.Logger,
		activeMetric: o.MetricsController.RegisterGauge("antispam_active",
			"Gauge indicates whether the antispam is enabled",
		),
		banMetric: o.MetricsController.RegisterGaugeVec("antispam_banned",
			"Source is banned",
			"source_name",
		),
		exceptionMetric: o.MetricsController.RegisterCounterVec("antispam_exceptions",
			"How many times an exception match with an event",
			"name",
		),
	}

	var err error

	if o.ConfigV2 != nil {
		a.rules, a.threshold, err = extractAntispam(o.ConfigV2)
		if err != nil {
			o.Logger.Fatal("can't extract antispam", zap.Error(err))
		}
	} else {
		a.rules, err = exceptionToRules(o.Exceptions)
		if err != nil {
			o.Logger.Fatal("can't convert exceptions to rules")
		}

		if o.Threshold > 0 {
			a.threshold = o.Threshold
		} else {
			a.threshold = thresholdUnlimited
		}
	}

	a.enabled = a.threshold != thresholdUnlimited

	for i := range a.rules {
		a.rules[i].Prepare(i)
		a.enabled = a.enabled || a.rules[i].Threshold != thresholdUnlimited
	}

	// not enabled by default
	a.activeMetric.Set(0)

	return a
}

func (a *Antispammer) IsSpam(
	id string,
	name string,
	isNewSource bool,
	event []byte,
	timeEvent time.Time,
	meta map[string]string,
) bool {
	if !a.enabled {
		return false
	}

	rlMapKey := id
	threshold := a.threshold

	for i := range a.rules {
		rule := &a.rules[i]
		if rule.Condition.check(event, []byte(name), meta) {
			switch rule.Threshold {
			case thresholdUnlimited:
				if rule.Name != "" {
					a.exceptionMetric.WithLabelValues(rule.Name).Inc()
				}
				return false
			case thresholdBlocked:
				return true
			}

			rlMapKey = rule.RLMapKey
			threshold = rule.Threshold
			break
		}
	}

	switch threshold {
	case thresholdUnlimited:
		return false
	case thresholdBlocked:
		return true
	}

	a.mu.RLock()
	src, has := a.sources[rlMapKey]
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
	if x == int32(threshold) {
		src.counter.Swap(int32(a.unbanIterations * threshold))
		a.activeMetric.Set(1)
		a.banMetric.WithLabelValues(name).Inc()
		a.logger.Warn("source has been banned",
			zap.Any("id", id), zap.String("name", name),
			zap.Time("time_event", timeEvent), zap.Int64("diff_nsec", diff),
			zap.Int64("maintenance_nsec", a.maintenanceInterval.Nanoseconds()),
			zap.Int32("counter", src.counter.Load()),
		)
	}

	return x >= int32(threshold)
}

func (a *Antispammer) Maintenance() {
	a.mu.Lock()

	allUnbanned := true
	for sourceID, source := range a.sources {
		x := int(source.counter.Load())

		if x == 0 {
			delete(a.sources, sourceID)
			a.banMetric.DeleteLabelValues(source.name)
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

type Exception struct {
	matchrule.RuleSet
	CheckSourceName bool `json:"check_source_name"`
}

type Exceptions []Exception

func (e Exceptions) Prepare() {
	for i := range e {
		e[i].Prepare()
	}
}
