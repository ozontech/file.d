package stats

import (
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/version"
	prom "github.com/prometheus/client_golang/prometheus"
)

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
	allMetrics map[key]prom.Counter
}

const (
	subsystemName = "stats"

	unknownCounter   = "unknown_counter"
	duplicateCounter = "duplicate_counter"
)

var statsGlobal *stats

func InitStats() {
	if statsGlobal != nil {
		logger.Error("attempt to initialize stats more than once")
		return
	}

	statsGlobal = &stats{
		allMetrics: make(map[key]prom.Counter),
		// This map will store prom.Counter instead of prom.Collector for now
		// because typecasts are slow.
		// A viable option would be to use a map of maps if we ever need
		// to be able to store several metric types.
	}

	statsGlobal.registerOwnMetrics()
}

func RegisterCounter(metricDesc *MetricDesc) {
	maskPromCounter := prom.NewCounter(prom.CounterOpts{
		Namespace:   pipeline.PromNamespace,
		Subsystem:   metricDesc.Subsystem,
		Name:        metricDesc.Name,
		Help:        metricDesc.Help,
		ConstLabels: map[string]string{"version": version.AppVersion},
	})

	keyInternal := key{pipeline.PromNamespace, metricDesc.Subsystem, metricDesc.Name}
	registerMetric(keyInternal, maskPromCounter)
}

// Returns counter of given metric, than can be inc or dec.
func GetCounter(subsystem, metricName string) prom.Counter {
	checkStatsInitialized()

	if val, ok := statsGlobal.allMetrics[getKey(subsystem, metricName)]; ok {
		return val
	}

	return statsGlobal.allMetrics[key{pipeline.PromNamespace, subsystem, metricName}]
}

func registerMetric(k key, metric prom.Counter) { // todo: support prom.Collector
	checkStatsInitialized()

	if _, ok := statsGlobal.allMetrics[k]; ok {
		logger.Errorf("rewriting existent metric")
		GetCounter(subsystemName, duplicateCounter).Inc()
	}

	statsGlobal.allMetrics[k] = metric
	prom.DefaultRegisterer.Unregister(metric)
	prom.DefaultRegisterer.MustRegister(metric)
}

func (s *stats) registerOwnMetrics() {
	RegisterCounter(&MetricDesc{
		Subsystem: subsystemName,
		Name:      unknownCounter,
		Help:      "Counter for non-existent metrics",
	})
	RegisterCounter(&MetricDesc{
		Subsystem: subsystemName,
		Name:      duplicateCounter,
		Help:      "Counter for duplicate metrics",
	})
}

func getKey(subsystem, metricName string) key {
	return key{pipeline.PromNamespace, subsystem, metricName}
}

func checkStatsInitialized() {
	if statsGlobal == nil {
		logger.Panicf("stats package uninitialized")
	}
}
