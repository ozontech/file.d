package stats

import (
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/logger"

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

	unregisteredCounter = "unknown_counter"
	unregisteredGauge   = "unknown_gauge"
	duplicateCounter    = "duplicate_counter"
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
		ConstLabels: map[string]string{"version": buildinfo.Version},
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
		ConstLabels: map[string]string{"version": buildinfo.Version},
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

	logger.Errorf("attempt to access an unregistered metric, name=%s. returning the unknown_counter", metricName)
	// in case somebody is trying to increment a non-registered metric,
	// an "unregistered" counter will be incremented.
	// this way file.d won't fail if the plugin developer
	// forgot to register a metric before using it.
	// same thing happens with gauges
	return statsGlobal.counters[getKey(subsystemName, unregisteredCounter)]
}

// GetGauge returns gauge for a given metric.
func GetGauge(subsystem, metricName string) prom.Gauge {
	checkStatsInitialized()

	if val, ok := statsGlobal.gauges[getKey(subsystem, metricName)]; ok {
		return val
	}

	logger.Errorf("attempt to access an unregistered metric, name=%s. returning the unknown_gauge", metricName)
	return statsGlobal.gauges[getKey(subsystemName, unregisteredGauge)]
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
		Name:      unregisteredCounter,
		Help:      "Counter for unregistered metrics",
	})
	RegisterGauge(&MetricDesc{
		Subsystem: subsystemName,
		Name:      unregisteredGauge,
		Help:      "Gauge for unregistered metrics",
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
