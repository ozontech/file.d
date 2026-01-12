package metric

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	PromNamespace = "file_d"
)

var (
	SecondsBucketsDetailedNano = prometheus.ExponentialBuckets(0.000005, 2, 19) // covers range from 5ns to 1.3ms
	SecondsBucketsDetailed     = prometheus.ExponentialBuckets(0.0005, 2, 16)   // covers range from 500us to 16.384s
	SecondsBucketsLong         = prometheus.ExponentialBuckets(0.005, 2, 16)    // covers range from 5ms to 163.84s
)

type Ctl struct {
	subsystem string
	register  *prometheus.Registry

	holder  *Holder
	metrics map[string]prometheus.Collector
	mu      sync.RWMutex
}

func NewCtl(subsystem string, registry *prometheus.Registry, metricHoldDuration time.Duration) *Ctl {
	ctl := &Ctl{
		subsystem: subsystem,
		register:  registry,
		metrics:   make(map[string]prometheus.Collector),
	}

	if metricHoldDuration != 0 {
		ctl.holder = NewHolder(metricHoldDuration)
	}

	return ctl
}

func (mc *Ctl) Maintenance() {
	if mc.holder == nil {
		return
	}

	mc.holder.maintenance()
}

func (mc *Ctl) AddToHolder(mv heldMetricVec) {
	if mc.holder == nil {
		return
	}

	mc.holder.addMetricVec(mv)
}

func (mc *Ctl) RegisterCounter(name, help string) *Counter {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	})

	return newCounter(mc.registerMetric(name, counter).(prometheus.Counter))
}

func (mc *Ctl) RegisterCounterVec(name, help string, labels ...string) *CounterVec {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	return newCounterVec(mc.registerMetric(name, counterVec).(*prometheus.CounterVec))
}

func (mc *Ctl) RegisterGauge(name, help string) *Gauge {
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	})

	return newGauge(mc.registerMetric(name, gauge).(prometheus.Gauge))
}

func (mc *Ctl) RegisterGaugeVec(name, help string, labels ...string) *GaugeVec {
	gaugeVec := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
	}, labels)

	return newGaugeVec(mc.registerMetric(name, gaugeVec).(*prometheus.GaugeVec))
}

func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64) *Histogram {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	})

	return newHistogram(mc.registerMetric(name, histogram).(prometheus.Histogram))
}

func (mc *Ctl) RegisterHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	histogramVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: PromNamespace,
		Subsystem: mc.subsystem,
		Name:      name,
		Help:      help,
		Buckets:   buckets,
	}, labels)

	return newHistogramVec(mc.registerMetric(name, histogramVec).(*prometheus.HistogramVec))
}

func (mc *Ctl) registerMetric(name string, newMetric prometheus.Collector) prometheus.Collector {
	mc.mu.RLock()
	metric, has := mc.metrics[name]
	mc.mu.RUnlock()
	if has {
		return metric
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()
	metric, has = mc.metrics[name]
	if !has {
		metric = newMetric
		mc.metrics[name] = metric
		mc.register.MustRegister(metric)
	}

	return metric
}
