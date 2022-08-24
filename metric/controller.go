package metric

import (
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/logger"
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"

	metricDuplicateCounter = "metric_duplicate_counter"
)

type MetricsCtl struct {
	subsystem string
	counters  map[string]*prom.CounterVec
	gauges    map[string]*prom.GaugeVec
}

func New(subsystem string) *MetricsCtl {
	ctl := &MetricsCtl{
		subsystem: subsystem,
		counters:  make(map[string]*prom.CounterVec),
		gauges:    make(map[string]*prom.GaugeVec),
	}
	ctl.RegisterCounter(metricDuplicateCounter, "Counter for duplicate metrics", "source")
	return ctl
}

func (mc *MetricsCtl) RegisterCounter(name, help string, labels ...string) *prom.CounterVec {
	promCounter := prom.NewCounterVec(prom.CounterOpts{
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	}, labels)

	if _, hasCounter := mc.counters[name]; hasCounter {
		mc.incMetricDuplicate(name)
		return mc.counters[name]
	}

	mc.counters[name] = promCounter
	prom.DefaultRegisterer.Unregister(promCounter)
	prom.DefaultRegisterer.MustRegister(promCounter)
	return promCounter
}

func (mc *MetricsCtl) RegisterGauge(name, help string, labels ...string) *prom.GaugeVec {
	promGauge := prom.NewGaugeVec(prom.GaugeOpts{
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	}, labels)

	if _, hasGauge := mc.gauges[name]; hasGauge {
		mc.incMetricDuplicate(name)
		return mc.gauges[name]
	}

	mc.gauges[name] = promGauge
	prom.DefaultRegisterer.Unregister(promGauge)
	prom.DefaultRegisterer.MustRegister(promGauge)
	return promGauge
}

func (mc *MetricsCtl) incMetricDuplicate(name string) {
	logger.Errorf("rewriting existent metric")
	mc.counters[metricDuplicateCounter].WithLabelValues(name).Inc()
}
