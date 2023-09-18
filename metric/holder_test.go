package metric

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

const testHoldDuration = 3 * time.Second

func newTestHolder() *Holder {
	return NewHolder(testHoldDuration)
}

func Test_Holder_Maintenance(t *testing.T) {
	h := newTestHolder()

	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "maintenance",
		Name:      "counter_vec",
		Help:      "help counter_vec",
	}, []string{"lbl"})
	prometheus.MustRegister(counterVec)
	counterVecWrapper := h.AddCounterVec(counterVec)

	now := time.Now()

	counterVecWrapper.WithLabelValues("val1").counter.Inc()
	counterVecWrapper.WithLabelValues("val1").changeTime.Store(now.UnixNano())

	counterVecWrapper.WithLabelValues("val2").counter.Add(2)
	counterVecWrapper.WithLabelValues("val2").changeTime.Store(now.Add(-(testHoldDuration + time.Second)).UnixNano())

	assert.Equal(t, 2, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(1), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))
	assert.Equal(t, float64(2), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val2").counter))

	h.Maintenance()

	// counterVecWrapper lost metric with label `val2`, it's removed by holder
	assert.Equal(t, 1, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(1), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))

	// make metric with label `val2` active again
	counterVecWrapper.WithLabelValues("val2").counter.Add(3)

	assert.Equal(t, 2, testutil.CollectAndCount(counterVecWrapper.vec))
	assert.Equal(t, float64(1), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val1").counter))
	assert.Equal(t, float64(3), testutil.ToFloat64(counterVecWrapper.WithLabelValues("val2").counter))
}

func Test_Holder_AddCounterVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := newCounterVecWrapper(metric1)

	metric2 := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test_holder",
		Subsystem: "counter_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := newCounterVecWrapper(metric2)

	tests := []struct {
		name    string
		metric  *prometheus.CounterVec
		wrapper *CounterVecWrapper
	}{
		{
			name:    "without_labels",
			metric:  metric1,
			wrapper: wrapper1,
		},
		{
			name:    "with_labels",
			metric:  metric2,
			wrapper: wrapper2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddCounterVec(tt.metric))
		})
	}
}

func Test_Holder_AddGaugeVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := newGaugeVecWrapper(metric1)

	metric2 := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "test_holder",
		Subsystem: "gauge_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := newGaugeVecWrapper(metric2)

	tests := []struct {
		name    string
		metric  *prometheus.GaugeVec
		wrapper *GaugeVecWrapper
	}{
		{
			name:    "without_labels",
			metric:  metric1,
			wrapper: wrapper1,
		},
		{
			name:    "with_labels",
			metric:  metric2,
			wrapper: wrapper2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddGaugeVec(tt.metric))
		})
	}
}

func Test_Holder_AddHistogramVec(t *testing.T) {
	h := newTestHolder()

	metric1 := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram_vec",
		Name:      "metric1",
		Help:      "help1",
	}, nil)
	wrapper1 := newHistogramVecWrapper(metric1)

	metric2 := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "test_holder",
		Subsystem: "histogram_vec",
		Name:      "metric2",
		Help:      "help2",
	}, []string{"lbl1, lbl2"})
	wrapper2 := newHistogramVecWrapper(metric2)

	tests := []struct {
		name          string
		metric        *prometheus.HistogramVec
		wrapper       *HistogramVecWrapper
		holderMapSize int
	}{
		{
			name:    "without_labels",
			metric:  metric1,
			wrapper: wrapper1,
		},
		{
			name:    "with_labels",
			metric:  metric2,
			wrapper: wrapper2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wrapper, h.AddHistogramVec(tt.metric))
		})
	}
}
