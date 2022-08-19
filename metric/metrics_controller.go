package metric

import (
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/logger"

	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"

	unregisteredMetric = "metric_unknown_metric"
	duplicateCounter   = "metric_duplicate_counter"
)

type MetricsCtl struct {
	subsystem string
	counters  map[string]prom.Counter
	gauges    map[string]prom.Gauge
}

func New(subsystem string) *MetricsCtl {
	ctl := &MetricsCtl{
		subsystem: subsystem,
		counters:  make(map[string]prom.Counter),
		gauges:    make(map[string]prom.Gauge),
	}
	ctl.RegisterCounter(unregisteredMetric, "Counter for unregistered metrics")
	ctl.RegisterCounter(duplicateCounter, "Counter for duplicate metrics")
	return ctl
}

func (mc *MetricsCtl) RegisterCounter(name, help string) {
	promCounter := prom.NewCounter(prom.CounterOpts{
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	})

	if _, hasCounter := mc.counters[name]; hasCounter {
		logger.Errorf("rewriting existent metric")
		mc.IncCounter(duplicateCounter)
	}

	mc.counters[name] = promCounter
	prom.DefaultRegisterer.Unregister(promCounter)
	prom.DefaultRegisterer.MustRegister(promCounter)
}

func (mc *MetricsCtl) IncCounter(name string) {
	if counter, ok := mc.counters[name]; ok {
		counter.Inc()
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}

func (mc *MetricsCtl) AddCounter(name string, value float64) {
	if counter, ok := mc.counters[name]; ok {
		counter.Add(value)
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}

func (mc *MetricsCtl) RegisterGauge(name, help string) {
	promGauge := prom.NewGauge(prom.GaugeOpts{
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	})

	if _, hasGauge := mc.gauges[name]; hasGauge {
		logger.Errorf("rewriting existent metric")
		mc.IncCounter(duplicateCounter)
	}

	mc.gauges[name] = promGauge
	prom.DefaultRegisterer.Unregister(promGauge)
	prom.DefaultRegisterer.MustRegister(promGauge)
}

func (mc *MetricsCtl) IncGauge(name string) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Inc()
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}

func (mc *MetricsCtl) DecGauge(name string) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Dec()
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}

func (mc *MetricsCtl) AddGauge(name string, value float64) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Add(value)
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}

func (mc *MetricsCtl) SetGauge(name string, value float64) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Set(value)
	} else {
		mc.counters[unregisteredMetric].Inc()
	}
}
