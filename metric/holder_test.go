package metric

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

const testHoldDuration = 3 * time.Second

func newTestHolder() *Holder {
	return NewHolder(prometheus.NewRegistry(), testHoldDuration)
}

func TestHolder_Maintenance(t *testing.T) {
	h := newTestHolder()
	go h.registerMetrics()

	startTime := time.Now()
	setNowTime(startTime)

	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "maintenance",
		Name:      "counter",
		Help:      "help counter",
	})
	h.registry.MustRegister(counter)
	counterWrapper := h.AddCounter(counter)

	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "maintenance",
		Name:      "counter_vec",
		Help:      "help counter_vec",
	}, []string{"lbl"})
	h.registry.MustRegister(counterVec)
	counterVecWrapper := h.AddCounterVec(counterVec)

	counterWrapper.Add(2)
	counterVecWrapper.WithLabelValues("val1").Inc()
	counterVecWrapper.WithLabelValues("val2").Add(2)

	assert.Equal(t, float64(2), testutil.ToFloat64(counterWrapper.counter))
	assert.Equal(t, true, counterWrapper.active)

	assert.Equal(t, 2, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(1), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))
	assert.Equal(t, float64(2), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val2").counter))

	setNowTime(startTime.Add(testHoldDuration - time.Second))

	// use one metric, so it is not will be removed by holder
	counterVecWrapper.WithLabelValues("val1").Inc()

	setNowTime(startTime.Add(testHoldDuration + time.Second))
	h.Maintenance()

	// counterWrapper became inactive (unregister by holder), but its value is preserved.
	assert.Equal(t, float64(2), testutil.ToFloat64(counterWrapper.counter))
	assert.Equal(t, false, counterWrapper.active)

	// counterVecWrapper lost metric with label `val2`, its remove by holder.
	assert.Equal(t, 1, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(2), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))

	// use other metrics to make them active again
	counterWrapper.Add(3)
	counterVecWrapper.WithLabelValues("val2").Inc()

	assert.Equal(t, float64(5), testutil.ToFloat64(counterWrapper.counter))
	assert.Equal(t, true, counterWrapper.active)

	assert.Equal(t, 2, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(2), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))
	assert.Equal(t, float64(1), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val2").counter))

	close(h.regChan)
}

func TestHolder_AddCounter(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter",
		Name:      "metric1",
		Help:      "help1",
	})
	wrapper1 := &CounterWrapper{
		counter: metric1,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter",
		Name:      "metric2",
		Help:      "help2",
	})
	wrapper2 := &CounterWrapper{
		counter: metric2,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        prometheus.Counter
		wrapper       *CounterWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddCounter(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.counters))
		})
	}
}

func TestHolder_AddCounterVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := &CounterVecWrapper{
		vec: metric1,
		wrapperVec: wrapperVec[CounterWrapper]{
			elems:     make(map[prometheus.Metric]*CounterWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := &CounterVecWrapper{
		vec: metric2,
		wrapperVec: wrapperVec[CounterWrapper]{
			elems:     make(map[prometheus.Metric]*CounterWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        *prometheus.CounterVec
		wrapper       *CounterVecWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddCounterVec(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.counterVecs))
		})
	}
}

func TestHolder_AddGauge(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge",
		Name:      "metric1",
		Help:      "help1",
	})
	wrapper1 := &GaugeWrapper{
		gauge: metric1,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge",
		Name:      "metric2",
		Help:      "help2",
	})
	wrapper2 := &GaugeWrapper{
		gauge: metric2,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        prometheus.Gauge
		wrapper       *GaugeWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddGauge(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.gauges))
		})
	}
}

func TestHolder_AddGaugeVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := &GaugeVecWrapper{
		vec: metric1,
		wrapperVec: wrapperVec[GaugeWrapper]{
			elems:     make(map[prometheus.Metric]*GaugeWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := &GaugeVecWrapper{
		vec: metric2,
		wrapperVec: wrapperVec[GaugeWrapper]{
			elems:     make(map[prometheus.Metric]*GaugeWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        *prometheus.GaugeVec
		wrapper       *GaugeVecWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddGaugeVec(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.gaugeVecs))
		})
	}
}

func TestHolder_AddHistogram(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram",
		Name:      "metric1",
		Help:      "help1",
	})
	wrapper1 := &HistogramWrapper{
		histogram: metric1,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram",
		Name:      "metric2",
		Help:      "help2",
	})
	wrapper2 := &HistogramWrapper{
		histogram: metric2,
		wrapper: wrapper{
			active:    true,
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        prometheus.Histogram
		wrapper       *HistogramWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddHistogram(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.histograms))
		})
	}
}

func TestHolder_AddHistogramVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := &HistogramVecWrapper{
		vec: metric1,
		wrapperVec: wrapperVec[HistogramWrapper]{
			elems:     make(map[prometheus.Metric]*HistogramWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}
	metric2 := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := &HistogramVecWrapper{
		vec: metric2,
		wrapperVec: wrapperVec[HistogramWrapper]{
			elems:     make(map[prometheus.Metric]*HistogramWrapper),
			mu:        new(sync.RWMutex),
			registrar: h.regChan,
		},
	}

	tests := []struct {
		name          string
		metric        *prometheus.HistogramVec
		wrapper       *HistogramVecWrapper
		holderMapSize int
	}{
		{
			name:          "first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 1,
		},
		{
			name:          "second",
			metric:        metric2,
			wrapper:       wrapper2,
			holderMapSize: 2,
		},
		{
			name:          "again_first",
			metric:        metric1,
			wrapper:       wrapper1,
			holderMapSize: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddHistogramVec(tt.metric))
			assert.Equal(t, tt.holderMapSize, len(h.metrics.histogramVecs))
		})
	}
}
