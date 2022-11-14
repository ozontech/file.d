package metric

import (
	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

// Ctl creates, stores and returns metrics of different types.
type Ctl struct {
	subsystem  string
	counters   map[string]*prom.CounterVec
	gauges     map[string]*prom.GaugeVec
	histograms map[string]*prom.HistogramVec
}

// New returns instance of metric.Ctl.
func New(subsystem string) *Ctl {
	ctl := &Ctl{
		subsystem:  subsystem,
		counters:   make(map[string]*prom.CounterVec),
		gauges:     make(map[string]*prom.GaugeVec),
		histograms: make(map[string]*prom.HistogramVec),
	}

	return ctl
}

// RegisterCounter returns new or already existed counter.
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

// RegisterGauge returns new or already existed gauge.
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

// RegisterHistogram returns new or already existed histogram.
func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64, labels ...string) *prom.HistogramVec {
	if histogram, hasHistogram := mc.histograms[name]; hasHistogram {
		return histogram
	}

	promHistogram := prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	}, labels)

	mc.histograms[name] = promHistogram
	prom.DefaultRegisterer.Unregister(promHistogram)
	prom.DefaultRegisterer.MustRegister(promHistogram)

	return promHistogram
}
