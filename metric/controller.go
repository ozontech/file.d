package metric

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

var (
	SecondsBucketsDetailed = prometheus.ExponentialBuckets(0.0005, 2, 16) // covers range from 500us to 16.384s
	SecondsBucketsLong     = prometheus.ExponentialBuckets(0.005, 2, 16)  // covers range from 5ms to 163.84s
)

type Ctl struct {
	subsystem string
	register  *prometheus.Registry

	counters  map[string]*prometheus.CounterVec
	counterMx *sync.Mutex

	gauges  map[string]*prometheus.GaugeVec
	gaugeMx *sync.Mutex

	histograms  map[string]*prometheus.HistogramVec
	histogramMx *sync.Mutex
}

func New(subsystem string, registry *prometheus.Registry) *Ctl {
	ctl := &Ctl{
		subsystem:   subsystem,
		counters:    make(map[string]*prometheus.CounterVec),
		counterMx:   new(sync.Mutex),
		gauges:      make(map[string]*prometheus.GaugeVec),
		gaugeMx:     new(sync.Mutex),
		histograms:  make(map[string]*prometheus.HistogramVec),
		histogramMx: new(sync.Mutex),
		register:    registry,
	}
	return ctl
}

func (mc *Ctl) RegisterCounter(name, help string, labels ...string) *prometheus.CounterVec {
	mc.counterMx.Lock()
	defer mc.counterMx.Unlock()

	if metric, hasCounter := mc.counters[name]; hasCounter {
		return metric
	}

	promCounter := prometheus.NewCounterVec(prometheus.CounterOpts{
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

func (mc *Ctl) RegisterGauge(name, help string, labels ...string) *prometheus.GaugeVec {
	mc.gaugeMx.Lock()
	defer mc.gaugeMx.Unlock()

	if metric, hasGauge := mc.gauges[name]; hasGauge {
		return metric
	}

	promGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
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

func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	mc.histogramMx.Lock()
	defer mc.histogramMx.Unlock()

	if metric, hasHistogram := mc.histograms[name]; hasHistogram {
		return metric
	}

	promHistogram := prometheus.NewHistogramVec(prometheus.HistogramOpts{
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
