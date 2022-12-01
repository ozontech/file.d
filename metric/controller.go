package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

type Ctl struct {
	subsystem string
	register  *prom.Registry
	counters  map[string]*prom.CounterVec
	gauges    map[string]*prom.GaugeVec
}

func New(subsystem string, registry *prom.Registry) *Ctl {
	ctl := &Ctl{
		subsystem: subsystem,
		counters:  make(map[string]*prom.CounterVec),
		gauges:    make(map[string]*prom.GaugeVec),
		register:  registry,
	}
	return ctl
}

func (mc *Ctl) RegisterCounter(name, help string, labels ...string) *prom.CounterVec {
	if metric, hasCounter := mc.counters[name]; hasCounter {
		return metric
	}

	promCounter := prom.NewCounterVec(prom.CounterOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	mc.counters[name] = promCounter
	mc.register.Unregister(promCounter)
	mc.register.MustRegister(promCounter)
	return promCounter
}

func (mc *Ctl) RegisterGauge(name, help string, labels ...string) *prom.GaugeVec {
	if metric, hasGauge := mc.gauges[name]; hasGauge {
		return metric
	}

	promGauge := prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	mc.gauges[name] = promGauge
	mc.register.Unregister(promGauge)
	mc.register.MustRegister(promGauge)
	return promGauge
}
