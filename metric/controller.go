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
	SecondsBucketsDetailedNano = prometheus.ExponentialBuckets(0.000005, 2.18, 17) // covers range from 5µs to 1.3s
	SecondsBucketsDetailed     = prometheus.ExponentialBuckets(0.0005, 2, 16)      // covers range from 500µs to 16.384s
	SecondsBucketsLong         = prometheus.ExponentialBuckets(0.005, 2, 16)       // covers range from 5000µs to 163.84s
)

type Ctl struct {
	subsystem string
	register  *prometheus.Registry

	holder                    *Holder
	metrics                   map[string]any
	metricMaxLabelValueLength int
	mu                        sync.RWMutex
}

func NewCtl(subsystem string, registry *prometheus.Registry, metricHoldDuration time.Duration, metricMaxLabelValueLength int) *Ctl {
	ctl := &Ctl{
		subsystem:                 subsystem,
		register:                  registry,
		metrics:                   make(map[string]any),
		metricMaxLabelValueLength: metricMaxLabelValueLength,
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
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewCounter(prometheus.CounterOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
			})
		},
		func(c prometheus.Collector) *Counter {
			return newCounter(c.(prometheus.Counter))
		},
	)
}

func (mc *Ctl) RegisterCounterVec(name, help string, labels ...string) *CounterVec {
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewCounterVec(prometheus.CounterOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
			}, labels)
		},
		func(c prometheus.Collector) *CounterVec {
			return newCounterVec(c.(*prometheus.CounterVec), mc.metricMaxLabelValueLength)
		},
	)
}

func (mc *Ctl) RegisterGauge(name, help string) *Gauge {
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
			})
		},
		func(c prometheus.Collector) *Gauge {
			return newGauge(c.(prometheus.Gauge))
		},
	)
}

func (mc *Ctl) RegisterGaugeVec(name, help string, labels ...string) *GaugeVec {
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewGaugeVec(prometheus.GaugeOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
			}, labels)
		},
		func(c prometheus.Collector) *GaugeVec {
			return newGaugeVec(c.(*prometheus.GaugeVec), mc.metricMaxLabelValueLength)
		},
	)
}

func (mc *Ctl) RegisterHistogram(name, help string, buckets []float64) *Histogram {
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewHistogram(prometheus.HistogramOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
				Buckets:   buckets,
			})
		},
		func(c prometheus.Collector) *Histogram {
			return newHistogram(c.(prometheus.Histogram))
		},
	)
}

func (mc *Ctl) RegisterHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	return registerWrapper(mc, name,
		func() prometheus.Collector {
			return prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Namespace: PromNamespace,
				Subsystem: mc.subsystem,
				Name:      name,
				Help:      help,
				Buckets:   buckets,
			}, labels)
		},
		func(c prometheus.Collector) *HistogramVec {
			return newHistogramVec(c.(*prometheus.HistogramVec), mc.metricMaxLabelValueLength)
		},
	)
}

// registerWrapper returns a cached wrapper by name or creates, registers and caches a new one.
// The prometheus collector is created lazily only on a cache miss.
func registerWrapper[W any](
	mc *Ctl,
	name string,
	newCollector func() prometheus.Collector,
	wrap func(prometheus.Collector) W,
) W {
	mc.mu.RLock()
	cached, has := mc.metrics[name]
	mc.mu.RUnlock()
	if has {
		return cached.(W)
	}

	mc.mu.Lock()
	defer mc.mu.Unlock()

	if cached, has = mc.metrics[name]; has {
		return cached.(W)
	}

	collector := newCollector()
	mc.register.MustRegister(collector)

	wrapper := wrap(collector)
	mc.metrics[name] = wrapper
	return wrapper
}
