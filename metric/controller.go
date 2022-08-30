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
	promCounter := prom.NewCounterVec(prom.CounterOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	if metric, hasCounter := mc.counters[name]; hasCounter {
		return metric
	}

	mc.counters[name] = promCounter
	prom.DefaultRegisterer.Unregister(promCounter)
	prom.DefaultRegisterer.MustRegister(promCounter)
	return promCounter
}

func (mc *Ctl) RegisterGauge(name, help string, labels ...string) *prom.GaugeVec {
	promGauge := prom.NewGaugeVec(prom.GaugeOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	if metric, hasGauge := mc.gauges[name]; hasGauge {
		return metric
	}

	mc.gauges[name] = promGauge
	prom.DefaultRegisterer.Unregister(promGauge)
	prom.DefaultRegisterer.MustRegister(promGauge)
	return promGauge
}

func (mc *Ctl) UnregisterMetrics() {
	for key, metric := range mc.counters {
		prom.Unregister(metric)
		delete(mc.counters, key)
	}
	for key, metric := range mc.gauges {
		prom.Unregister(metric)
		delete(mc.counters, key)
	}
}
