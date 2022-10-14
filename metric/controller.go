package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

type Ctl struct {
	subsystem string
	counters  map[string]*prom.CounterVec
	gauges    map[string]*prom.GaugeVec
}

func New(subsystem string) *Ctl {
	ctl := &Ctl{
		subsystem: subsystem,
		counters:  make(map[string]*prom.CounterVec),
		gauges:    make(map[string]*prom.GaugeVec),
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
	prom.DefaultRegisterer.Unregister(promCounter)
	prom.DefaultRegisterer.MustRegister(promCounter)
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
	prom.DefaultRegisterer.Unregister(promGauge)
	prom.DefaultRegisterer.MustRegister(promGauge)
	return promGauge
}
