package stats

import (
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/version"

	prom "github.com/prometheus/client_golang/prometheus"
)

const PromNamespace = "file_d"

const (
	counterMetricType = 1
	gaugeMetricType   = 2
)

type metricType = int

type MetricDesc struct {
	Subsystem string
	Name      string
	Help      string
}

type key struct {
	namespace string
	subsystem string
	name      string
}

type stats struct {
	counters map[key]prom.Counter
	gauges   map[key]prom.Gauge
}

const (
	subsystemName = "stats"

	unknownCounter   = "unknown_counter"
	unknownGauge     = "unknown_gauge"
	duplicateCounter = "duplicate_counter"
)

var statsGlobal *stats

func InitStats() {
	if statsGlobal != nil {
		logger.Error("attempt to initialize stats more than once")
		return
	}

	statsGlobal = &stats{
		counters: make(map[key]prom.Counter),
		gauges:   make(map[key]prom.Gauge),
	}

	statsGlobal.registerOwnMetrics()
}

func RegisterGauge(metricDesc *MetricDesc) {
	maskPromGauge := prom.NewGauge(prom.GaugeOpts{
		Namespace:   PromNamespace,
		Subsystem:   metricDesc.Subsystem,
		Name:        metricDesc.Name,
		Help:        metricDesc.Help,
		ConstLabels: map[string]string{"version": version.AppVersion},
	})

	keyInternal := key{PromNamespace, metricDesc.Subsystem, metricDesc.Name}
	registerMetric(gaugeMetricType, keyInternal, maskPromGauge)
}

func RegisterCounter(metricDesc *MetricDesc) {
	maskPromCounter := prom.NewCounter(prom.CounterOpts{
		Namespace:   PromNamespace,
		Subsystem:   metricDesc.Subsystem,
		Name:        metricDesc.Name,
		Help:        metricDesc.Help,
		ConstLabels: map[string]string{"version": version.AppVersion},
	})

	keyInternal := key{PromNamespace, metricDesc.Subsystem, metricDesc.Name}
	registerMetric(counterMetricType, keyInternal, maskPromCounter)
}

// GetCounter returns counter for a given metric.
func GetCounter(subsystem, metricName string) prom.Counter {
	checkStatsInitialized()

	if val, ok := statsGlobal.counters[getKey(subsystem, metricName)]; ok {
		return val
	}

	return statsGlobal.counters[getKey(subsystem, unknownCounter)]
}

// GetGauge returns gauge for a given metric.
func GetGauge(subsystem, metricName string) prom.Gauge {
	checkStatsInitialized()

	if val, ok := statsGlobal.gauges[getKey(subsystem, metricName)]; ok {
		return val
	}

	return statsGlobal.gauges[getKey(subsystem, unknownGauge)]
}

func registerMetric(mType metricType, k key, metric prom.Collector) {
	checkStatsInitialized()

	_, hasCounter := statsGlobal.counters[k]
	_, hasGauge := statsGlobal.gauges[k]

	if hasCounter || hasGauge {
		logger.Errorf("rewriting existent metric")
		GetCounter(subsystemName, duplicateCounter).Inc()
	}

	switch mType {
	case counterMetricType:
		statsGlobal.counters[k] = metric.(prom.Counter)
	case gaugeMetricType:
		statsGlobal.gauges[k] = metric.(prom.Gauge)
	}

	prom.DefaultRegisterer.Unregister(metric)
	prom.DefaultRegisterer.MustRegister(metric)
}

func (s *stats) registerOwnMetrics() {
	RegisterCounter(&MetricDesc{
		Subsystem: subsystemName,
		Name:      unknownCounter,
		Help:      "Counter for non-existent metrics",
	})
	RegisterGauge(&MetricDesc{
		Subsystem: subsystemName,
		Name:      unknownGauge,
		Help:      "Gauge for non-existent metrics",
	})
	RegisterCounter(&MetricDesc{
		Subsystem: subsystemName,
		Name:      duplicateCounter,
		Help:      "Counter for duplicate metrics",
	})
}

func getKey(subsystem, metricName string) key {
	return key{
		namespace: PromNamespace,
		subsystem: subsystem,
		name:      metricName,
	}
}

func checkStatsInitialized() {
	if statsGlobal == nil {
		logger.Panicf("stats package uninitialized")
	}
}
