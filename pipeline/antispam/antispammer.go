package antispam

import (
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
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
	sourcesThresholds   map[string]int
	exceptions          Exceptions
	rules               Rules

	logger *zap.Logger

	// antispammer metrics
	activeMetric    *metric.Gauge
	banMetric       *metric.GaugeVec
	exceptionMetric *metric.CounterVec

	// debug antispam
	totalLogs     *metric.CounterVec
	sourceCounter *metric.GaugeVec
	debugSource   map[string]int
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
	Rules               Rules

	Logger            *zap.Logger
	MetricsController *metric.Ctl

	// debug antispam
	DebugSources []string
}

func NewAntispammer(o *Options) *Antispammer {
	if o.Threshold > 0 {
		o.Logger.Info("antispam enabled",
			zap.Int("threshold", o.Threshold),
			zap.Duration("maintenance", o.MaintenanceInterval))
	}

	a := &Antispammer{
		unbanIterations:     o.UnbanIterations,
		threshold:           o.Threshold,
		maintenanceInterval: o.MaintenanceInterval,
		sources:             make(map[string]source),
		sourcesThresholds:   make(map[string]int),
		exceptions:          o.Exceptions,
		rules:               o.Rules,
		logger:              o.Logger,
		activeMetric: o.MetricsController.RegisterGauge("antispam_active",
			"Gauge indicates whether the antispam is enabled",
		),
		banMetric: o.MetricsController.RegisterGaugeVec("antispam_banned",
			"Source is banned",
			"source_name",
		),
		exceptionMetric: o.MetricsController.RegisterCounterVec("antispam_exceptions_total",
			"How many times an exception match with an event",
			"name",
		),

		totalLogs: o.MetricsController.RegisterCounterVec("antispam_events_total",
			"!Debug metric! Total events per source",
			"source_name",
		),
		sourceCounter: o.MetricsController.RegisterGaugeVec("antispam_source_counter_value",
			"!Debug metric! Antispam counter for sources contained in DebugSources",
			"source_name",
		),
		debugSource: make(map[string]int),
	}

	for _, source := range o.DebugSources {
		a.debugSource[source] = 1
	}

	// not enabled by default
	a.activeMetric.Set(0)

	return a
}

func (a *Antispammer) IsSpam(id string, name string, isNewSource bool, event []byte, timeEvent time.Time, meta map[string]string) bool {
	if _, ok := a.debugSource[name]; ok {
		a.totalLogs.WithLabelValues(name).Inc()
	}

	if a.rules == nil && a.threshold == -1 {
		return false
	}

	threshold := a.threshold
	if a.rules == nil {
		for i := 0; i < len(a.exceptions); i++ {
			e := &a.exceptions[i]
			checkData := event
			if e.CheckSourceName {
				checkData = []byte(name)
			}
			if e.Match(checkData) {
				if e.Name != "" {
					a.exceptionMetric.WithLabelValues(e.Name).Inc()
				}
				return false
			}
		}
	} else {
		for _, rule := range a.rules {
			data := &antispamData{
				eventBytes: event,
				sourceName: name,
				meta:       meta,
			}
			if rule.DoIfChecker.Check(data) {
				switch rule.Threshold {
				case thresholdUnlimited:
					if rule.Name != "" {
						a.exceptionMetric.WithLabelValues(rule.Name).Inc()
					}
					return false
				case thresholdBlocked:
					return true
				}

				threshold = rule.Threshold
				break
			}
		}
	}

	switch threshold {
	case thresholdUnlimited:
		return false
	case thresholdBlocked:
		return true
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
			a.sourcesThresholds[id] = threshold
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
			delete(a.sourcesThresholds, sourceID)
			a.banMetric.DeleteLabelValues(source.name)
			continue
		}

		threshold := a.sourcesThresholds[sourceID]
		isMore := x >= threshold
		x -= threshold
		if x < 0 {
			x = 0
		}

		if isMore && x < threshold {
			a.banMetric.WithLabelValues(source.name).Dec()
			a.logger.Info("source has been unbanned", zap.Any("id", sourceID))
		}

		if x >= threshold {
			allUnbanned = false
		}

		if x > a.unbanIterations*threshold {
			x = a.unbanIterations * threshold
		}

		source.counter.Swap(int32(x))

		if _, ok := a.debugSource[source.name]; ok {
			a.sourceCounter.WithLabelValues(source.name).Set(float64(x))
		}
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
