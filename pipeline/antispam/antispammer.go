package antispam

import (
	"fmt"
	"sync"
	"time"

	"sync/atomic"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"go.uber.org/zap"
)

const (
	thresholdUnlimited = -1
	thresholdBlocked   = 0
)

type SpamResult int

const (
	// event is not spam
	Passed SpamResult = iota
	// event is spam
	Dropped
	// event is spam, but passed by sampler
	Sampled
)

// partial state flags
const (
	psfDefault uint8 = 0

	psfPartial uint8 = 1 << (iota - 1) // chain of partial events
	psfDropped                         // first event in the chain of partial events was dropped
)

func checkMask(mask, flag uint8) bool {
	return mask&flag != 0
}

// Antispammer makes a decision on the need to parse the input log.
// It can be useful when any application writes logs at speed faster than File.d can read it.
//
// Anti-spammer supports exceptions for cases where you need to guarantee delivery of an important events.
type Antispammer struct {
	unbanIterations     int
	threshold           int
	maintenanceInterval time.Duration

	mu                   sync.RWMutex
	sources              map[string]source
	sourcesThresholds    map[string]int
	sourcesPartialStates map[string]uint8

	exceptions Exceptions
	rules      Rules

	sampler *Sampler
	logger  *zap.Logger

	// antispammer metrics
	activeMetric    *metric.Gauge
	banMetric       *metric.GaugeVec
	exceptionMetric *metric.CounterVec
	samplerMetric   *metric.CounterVec
}

type source struct {
	name          string
	counter       *atomic.Int32
	timestamp     *atomic.Int64
	sampleUntil   *atomic.Int64
	sampleCounter *atomic.Int64
}

type Options struct {
	MaintenanceInterval time.Duration
	Threshold           int
	UnbanIterations     int
	Exceptions          Exceptions
	Rules               Rules
	Sampler             *Sampler

	Logger            *zap.Logger
	MetricsController *metric.Ctl
}

type Sampler struct {
	Interval     time.Duration
	First        int64
	Thereafter   int64
	MarkerField  string
	MetricName   string
	MetricLabels []string
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

		sources:              make(map[string]source),
		sourcesThresholds:    make(map[string]int),
		sourcesPartialStates: make(map[string]uint8),

		exceptions: o.Exceptions,
		rules:      o.Rules,

		logger: o.Logger,

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
	}

	if o.Sampler != nil {
		a.sampler = o.Sampler
		o.Logger.Info("sampling of banned sources is enabled",
			zap.Duration("interval", o.Sampler.Interval),
			zap.Int64("first", o.Sampler.First),
			zap.Int64("thereafter", o.Sampler.Thereafter),
		)

		if o.Sampler.MetricName != "" {
			a.samplerMetric = o.MetricsController.RegisterCounterVec(
				o.Sampler.MetricName,
				"How many events from banned sources were let through by the sampler",
				o.Sampler.MetricLabels...,
			)
		}
	}
	// not enabled by default
	a.activeMetric.Set(0)

	return a
}

type SourceData struct {
	ID    string
	Name  string
	IsNew bool
}

type EventData struct {
	Bytes     []byte
	Time      time.Time
	IsPartial bool
}

func (a *Antispammer) IsSpam(sourceData SourceData, eventData EventData, meta map[string]string) SpamResult {
	if a.rules == nil && a.threshold == thresholdUnlimited {
		return Passed
	}

	a.mu.RLock()
	ps, has := a.sourcesPartialStates[sourceData.ID]
	a.mu.RUnlock()

	// for a chain of partial events, we check only the first event and save it's state.
	// for all other events from the chain, up to the last full one that completes this chain,
	// the state of the first one is used.
	if has && checkMask(ps, psfPartial) {
		dropped := checkMask(ps, psfDropped)
		if !eventData.IsPartial {
			// last event in chain
			a.mu.Lock()
			a.sourcesPartialStates[sourceData.ID] = psfDefault
			a.mu.Unlock()
		}
		if dropped {
			return Dropped
		}
		return Passed
	}

	if eventData.IsPartial {
		a.mu.Lock()
		a.sourcesPartialStates[sourceData.ID] = psfPartial
		a.mu.Unlock()
	}

	drop := func() SpamResult {
		if eventData.IsPartial {
			a.mu.Lock()
			a.sourcesPartialStates[sourceData.ID] |= psfDropped
			a.mu.Unlock()
		}
		return Dropped
	}

	threshold := a.threshold
	if a.rules == nil {
		for i := 0; i < len(a.exceptions); i++ {
			e := &a.exceptions[i]
			checkData := eventData.Bytes
			if e.CheckSourceName {
				checkData = []byte(sourceData.Name)
			}
			if e.Match(checkData) {
				if e.Name != "" {
					a.exceptionMetric.WithLabelValues(e.Name).Inc()
				}
				return Passed
			}
		}
	} else {
		data := &antispamData{
			eventBytes: eventData.Bytes,
			sourceName: sourceData.Name,
			meta:       meta,
		}
		for _, rule := range a.rules {
			if !rule.DoIfChecker.Check(data) {
				continue
			}
			switch rule.Threshold {
			case thresholdUnlimited:
				a.exceptionMetric.WithLabelValues(rule.Name).Inc()
				return Passed
			case thresholdBlocked:
				return drop()
			}

			threshold = rule.Threshold
			break
		}
	}

	switch threshold {
	case thresholdUnlimited:
		return Passed
	case thresholdBlocked:
		return drop()
	}

	a.mu.RLock()
	src, has := a.sources[sourceData.ID]
	a.mu.RUnlock()

	timeEventSeconds := eventData.Time.UnixNano()

	if !has {
		a.mu.Lock()
		if newSrc, has := a.sources[sourceData.ID]; has {
			src = newSrc
		} else {
			src = source{
				name:          sourceData.Name,
				counter:       &atomic.Int32{},
				timestamp:     &atomic.Int64{},
				sampleUntil:   &atomic.Int64{},
				sampleCounter: &atomic.Int64{},
			}
			src.timestamp.Add(timeEventSeconds)
			a.sources[sourceData.ID] = src
			a.sourcesThresholds[sourceData.ID] = threshold
		}
		a.mu.Unlock()
	}

	if sourceData.IsNew {
		src.counter.Swap(0)
		return Passed
	}

	x := src.counter.Load()
	diff := timeEventSeconds - src.timestamp.Swap(timeEventSeconds)
	if diff < a.maintenanceInterval.Nanoseconds() {
		x = src.counter.Add(1)
	}
	if x == int32(threshold) {
		src.counter.Swap(int32(a.unbanIterations * threshold))
		a.activeMetric.Set(1)
		a.banMetric.WithLabelValues(sourceData.Name).Inc()
		a.logger.Warn("source has been banned",
			zap.String("id", sourceData.ID), zap.String("name", sourceData.Name),
			zap.Time("time_event", eventData.Time), zap.Int64("diff_nsec", diff),
			zap.Int64("maintenance_nsec", a.maintenanceInterval.Nanoseconds()),
			zap.Int32("counter", src.counter.Load()),
		)
	}

	if x >= int32(threshold) {
		if a.sampler.isSampled(src) {
			return Sampled
		}
		return drop()
	}
	return Passed
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
			source.sampleCounter.Store(0)
			source.sampleUntil.Store(0)
		}

		if x >= threshold {
			allUnbanned = false
		}

		if x > a.unbanIterations*threshold {
			x = a.unbanIterations * threshold
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

func (a *Antispammer) SamplerMetric() *metric.CounterVec {
	return a.samplerMetric
}

func (s *Sampler) isSampled(src source) bool {
	if s == nil {
		return false
	}

	now := time.Now().UnixNano()
	if end := src.sampleUntil.Load(); now >= end {
		if src.sampleUntil.CompareAndSwap(end, now+s.Interval.Nanoseconds()) {
			src.sampleCounter.Store(0)
		}
	}

	val := src.sampleCounter.Add(1)
	if val <= s.First {
		return true
	}
	if s.Thereafter == 0 {
		return false
	}

	return (val-s.First)%s.Thereafter == 0
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
