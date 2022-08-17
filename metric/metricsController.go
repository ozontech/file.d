package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
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
	ctl.RegisterCounter(unregisteredCounter, "Counter for unregistered metrics")
	ctl.RegisterGauge(unregisteredGauge, "Gauge for unregistered metrics")
	return ctl
}

func (mc *MetricsCtl) RegisterCounter(name, help string) {
	RegisterCounter(&MetricDesc{
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	})
	mc.counters[name] = GetCounter(mc.subsystem, name)
}

func (mc *MetricsCtl) IncCounter(name string) {
	if counter, ok := mc.counters[name]; ok {
		counter.Inc()
	} else {
		mc.unregisteredCounter()
	}
}

func (mc *MetricsCtl) AddCounter(name string, value float64) {
	if counter, ok := mc.counters[name]; ok {
		counter.Add(value)
	} else {
		mc.unregisteredCounter()
	}
}

func (mc *MetricsCtl) RegisterGauge(name, help string) {
	RegisterGauge(&MetricDesc{
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	})
	mc.gauges[name] = GetGauge(mc.subsystem, name)
}

func (mc *MetricsCtl) IncGauge(name string) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Inc()
	} else {
		mc.unregisteredGauge()
	}
}

func (mc *MetricsCtl) DecGauge(name string) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Dec()
	} else {
		mc.unregisteredGauge()
	}
}

func (mc *MetricsCtl) AddGauge(name string, value float64) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Add(value)
	} else {
		mc.unregisteredGauge()
	}
}

func (mc *MetricsCtl) SetGauge(name string, value float64) {
	if gauge, ok := mc.gauges[name]; ok {
		gauge.Set(value)
	} else {
		mc.unregisteredGauge()
	}
}

func (mc *MetricsCtl) unregisteredCounter() {
	mc.counters[unregisteredCounter].Inc()
}

func (mc *MetricsCtl) unregisteredGauge() {
	mc.counters[unregisteredGauge].Inc()
}
