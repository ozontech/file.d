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

	counters  map[string]prometheus.Counter
	counterMx *sync.RWMutex

	counterVecs  map[string]*prometheus.CounterVec
	counterVecMx *sync.RWMutex

	gauges  map[string]prometheus.Gauge
	gaugeMx *sync.RWMutex

	gaugeVecs  map[string]*prometheus.GaugeVec
	gaugeVecMx *sync.RWMutex

	histograms  map[string]prometheus.Histogram
	histogramMx *sync.RWMutex

	histogramVecs  map[string]*prometheus.HistogramVec
	histogramVecMx *sync.RWMutex
}

func NewCtl(subsystem string, registry *prometheus.Registry) *Ctl {
	ctl := &Ctl{
		subsystem:      subsystem,
		register:       registry,
		counters:       make(map[string]prometheus.Counter),
		counterMx:      new(sync.RWMutex),
		counterVecs:    make(map[string]*prometheus.CounterVec),
		counterVecMx:   new(sync.RWMutex),
		gauges:         make(map[string]prometheus.Gauge),
		gaugeMx:        new(sync.RWMutex),
		gaugeVecs:      make(map[string]*prometheus.GaugeVec),
		gaugeVecMx:     new(sync.RWMutex),
		histograms:     make(map[string]prometheus.Histogram),
		histogramMx:    new(sync.RWMutex),
		histogramVecs:  make(map[string]*prometheus.HistogramVec),
		histogramVecMx: new(sync.RWMutex),
	}
	return ctl
}

func (mc *Ctl) RegisterCounter(name, help string) prometheus.Counter {
	newCounter := func() prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
		})
	}

	return registerMetric(mc.register, name, mc.counters, mc.counterMx, newCounter)
}

func (mc *Ctl) RegisterCounterVec(name, help string, labels ...string) *prometheus.CounterVec {
	newCounterVec := func() *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
		}, labels)
	}

	return registerMetric(mc.register, name, mc.counterVecs, mc.counterVecMx, newCounterVec)
}

func (mc *Ctl) RegisterGauge(name, help string) prometheus.Gauge {
	newGauge := func() prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
		})
	}

	return registerMetric(mc.register, name, mc.gauges, mc.gaugeMx, newGauge)
}

func (mc *Ctl) RegisterGaugeVec(name, help string, labels ...string) *prometheus.GaugeVec {
	newGaugeVec := func() *prometheus.GaugeVec {
		return prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
		}, labels)
	}

	return registerMetric(mc.register, name, mc.gaugeVecs, mc.gaugeVecMx, newGaugeVec)
}

func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64) prometheus.Histogram {
	newHistogram := func() prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		})
	}

	return registerMetric(mc.register, name, mc.histograms, mc.histogramMx, newHistogram)
}

func (mc *Ctl) RegisterHistogramVec(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	newHistogramVec := func() *prometheus.HistogramVec {
		return prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: PromNamespace,
			Subsystem: mc.subsystem,
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		}, labels)
	}

	return registerMetric(mc.register, name, mc.histogramVecs, mc.histogramVecMx, newHistogramVec)
}

func registerMetric[T prometheus.Collector](register *prometheus.Registry, name string,
	metrics map[string]T, mx *sync.RWMutex, newMetric func() T,
) T {
	mx.RLock()
	metric, has := metrics[name]
	mx.RUnlock()
	if has {
		return metric
	}

	mx.Lock()
	defer mx.Unlock()
	metric, has = metrics[name]
	if !has {
		metric = newMetric()

		metrics[name] = metric
		register.Unregister(metric)
		register.MustRegister(metric)
	}

	return metric
}
