package stats

import (
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

type AnyMetric prom.Collector

type stats struct {
	allMetrics map[key]AnyMetric
}

var statsGlobal *stats

func InitStats() {
	statsGlobal = &stats{}
	statsGlobal.allMetrics = make(map[key]AnyMetric)
}

func RegisterCounter(metricDesc *MetricDesc) {
	maskPromCounter := prom.NewCounter(prom.CounterOpts{
		Namespace: pipeline.PromNamespace,
		Subsystem: metricDesc.Subsystem,
		Name:      metricDesc.Name,
		Help:      metricDesc.Help,
	})

	keyInternal := key{pipeline.PromNamespace, metricDesc.Subsystem, metricDesc.Name}
	statsGlobal.registerMetric(keyInternal, maskPromCounter)
}

func GetCounter(subsystem, metricName string) prom.Counter {
	return statsGlobal.allMetrics[getKey(subsystem, metricName)].(prom.Counter)
}

func getKey(subsystem, metricName string) key {
	return key{pipeline.PromNamespace, subsystem, metricName}
}

func (s *stats) registerMetric(k key, metric prom.Collector) {
	s.allMetrics[k] = metric
	prom.DefaultRegisterer.Unregister(metric)
	prom.DefaultRegisterer.MustRegister(metric)
}
