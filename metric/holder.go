package metric

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Holder struct {
	holdDuration   time.Duration
	metricWrappers metricWrappers
}

func NewHolder(holdDuration time.Duration) *Holder {
	return &Holder{
		holdDuration:   holdDuration,
		metricWrappers: newMetricWrappers(),
	}
}

func (h *Holder) Maintenance() {
	h.updateMetrics()
}

func (h *Holder) AddCounterVec(counterVec *prometheus.CounterVec) *CounterVecWrapper {
	return h.metricWrappers.addCounterVec(counterVec)
}

func (h *Holder) AddGaugeVec(gaugeVec *prometheus.GaugeVec) *GaugeVecWrapper {
	return h.metricWrappers.addGaugeVec(gaugeVec)
}

func (h *Holder) AddHistogramVec(histogramVec *prometheus.HistogramVec) *HistogramVecWrapper {
	return h.metricWrappers.addHistogramVec(histogramVec)
}

// updateMetrics delete old metrics, that aren't in use since last update.
func (h *Holder) updateMetrics() {
	h.metricWrappers.update(h.holdDuration)
}

type metricWrappers struct {
	counterWrappers   []*CounterVecWrapper
	gaugeWrappers     []*GaugeVecWrapper
	histogramWrappers []*HistogramVecWrapper
	mu                *sync.Mutex
}

func newMetricWrappers() metricWrappers {
	return metricWrappers{
		counterWrappers:   make([]*CounterVecWrapper, 0),
		gaugeWrappers:     make([]*GaugeVecWrapper, 0),
		histogramWrappers: make([]*HistogramVecWrapper, 0),
		mu:                new(sync.Mutex),
	}
}

func (mw *metricWrappers) addCounterVec(counterVec *prometheus.CounterVec) *CounterVecWrapper {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	cvw := newCounterVecWrapper(counterVec)
	mw.counterWrappers = append(mw.counterWrappers, cvw)

	return cvw
}

func (mw *metricWrappers) addGaugeVec(gaugeVec *prometheus.GaugeVec) *GaugeVecWrapper {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	gvw := newGaugeVecWrapper(gaugeVec)
	mw.gaugeWrappers = append(mw.gaugeWrappers, gvw)

	return gvw
}

func (mw *metricWrappers) addHistogramVec(histogramVec *prometheus.HistogramVec) *HistogramVecWrapper {
	mw.mu.Lock()
	defer mw.mu.Unlock()

	hvw := newHistogramVecWrapper(histogramVec)
	mw.histogramWrappers = append(mw.histogramWrappers, hvw)

	return hvw
}

func (mw *metricWrappers) update(holdDuration time.Duration) {
	for _, cvw := range mw.counterWrappers {
		cvw.updateState(holdDuration)
	}

	for _, gvw := range mw.gaugeWrappers {
		gvw.updateState(holdDuration)
	}

	for _, hvw := range mw.histogramWrappers {
		hvw.updateState(holdDuration)
	}
}
