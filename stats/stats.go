package stats

import (
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
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
	subsystemName  = "stats"
	unknownCounter = "unknown_counter"
)

var statsGlobal *stats

func InitStats() {
	statsGlobal = &stats{
		allMetrics: make(map[key]prom.Counter),
		// This map will store prom.Counter instead of prom.Collector for now
		// because typecasts are slow.
		// A viable option would be to use a map of maps if we ever need
		// to be able to store several metric types.
	}

	statsGlobal.registerUnknown()
}

func RegisterCounter(metricDesc *MetricDesc) {
	maskPromCounter := prom.NewCounter(prom.CounterOpts{
		Namespace: pipeline.PromNamespace,
		Subsystem: metricDesc.Subsystem,
		Name:      metricDesc.Name,
		Help:      metricDesc.Help,
	})

	keyInternal := key{pipeline.PromNamespace, metricDesc.Subsystem, metricDesc.Name}
	registerMetric(keyInternal, maskPromCounter)
}

func GetCounter(subsystem, metricName string) prom.Counter {
	if statsGlobal == nil {
		logger.Panicf("stats package uninitialized")
	}

	if val, ok := statsGlobal.allMetrics[getKey(subsystem, metricName)]; ok {
		return val
	}

	return statsGlobal.allMetrics[key{pipeline.PromNamespace, subsystem, metricName}]
}

func getKey(subsystem, metricName string) key {
	return key{pipeline.PromNamespace, subsystem, metricName}
}

func registerMetric(k key, metric prom.Counter) { // todo: support prom.Collector
	if statsGlobal == nil {
		logger.Panicf("stats package uninitialized")
	}

	statsGlobal.allMetrics[k] = metric
	prom.DefaultRegisterer.Unregister(metric)
	prom.DefaultRegisterer.MustRegister(metric)
}

func (s *stats) registerUnknown() {
	RegisterCounter(&MetricDesc{
		Subsystem: subsystemName,
		Name:      unknownCounter,
		Help:      "Counter for non-existent metrics",
	})
}
