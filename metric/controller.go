package metric

import (
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/logger"
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
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	}, labels)

	if _, hasCounter := mc.counters[name]; hasCounter {
		logger.Panicf("attempt to register counter twice name=%s, subsystem=%s", name, mc.subsystem)
	}

	mc.counters[name] = promCounter
	prom.DefaultRegisterer.Unregister(promCounter)
	prom.DefaultRegisterer.MustRegister(promCounter)
	return promCounter
}

func (mc *Ctl) RegisterGauge(name, help string, labels ...string) *prom.GaugeVec {
	promGauge := prom.NewGaugeVec(prom.GaugeOpts{
		Namespace:   PromNamespace,
		Subsystem:   mc.subsystem,
		Name:        name,
		Help:        help,
		ConstLabels: map[string]string{"version": buildinfo.Version},
	}, labels)

	if _, hasGauge := mc.gauges[name]; hasGauge {
		logger.Panicf("attempt to register gauge twice name=%s, subsystem=%s", name, mc.subsystem)
	}

	mc.gauges[name] = promGauge
	prom.DefaultRegisterer.Unregister(promGauge)
	prom.DefaultRegisterer.MustRegister(promGauge)
	return promGauge
}
