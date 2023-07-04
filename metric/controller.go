package metric

import (
	"sync"

	prom "github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

var (
	SecondsBucketsLong = prom.ExponentialBuckets(0.005, 2, 16) // covers range from 5ms to 167s
)

type Ctl struct {
	subsystem string
	register  *prom.Registry

	counters  map[string]*prom.CounterVec
	counterMx *sync.Mutex

	gauges  map[string]*prom.GaugeVec
	gaugeMx *sync.Mutex

	histograms  map[string]*prom.HistogramVec
	histogramMx *sync.Mutex
}

func New(subsystem string, registry *prom.Registry) *Ctl {
	ctl := &Ctl{
		subsystem:   subsystem,
		counters:    make(map[string]*prom.CounterVec),
		counterMx:   new(sync.Mutex),
		gauges:      make(map[string]*prom.GaugeVec),
		gaugeMx:     new(sync.Mutex),
		histograms:  make(map[string]*prom.HistogramVec),
		histogramMx: new(sync.Mutex),
		register:    registry,
	}
	return ctl
}

func (mc *Ctl) RegisterCounter(name, help string, labels ...string) *prom.CounterVec {
	mc.counterMx.Lock()
	defer mc.counterMx.Unlock()

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
	mc.gaugeMx.Lock()
	defer mc.gaugeMx.Unlock()

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

func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64, labels ...string) *prom.HistogramVec {
	mc.histogramMx.Lock()
	defer mc.histogramMx.Unlock()

	if metric, hasHistogram := mc.histograms[name]; hasHistogram {
		return metric
	}

	promHistogram := prom.NewHistogramVec(prom.HistogramOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	}, labels)

	mc.histograms[name] = promHistogram
	mc.register.Unregister(promHistogram)
	mc.register.MustRegister(promHistogram)
	return promHistogram
}
