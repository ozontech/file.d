package metric

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/xtime"
	"github.com/prometheus/client_golang/prometheus"
)

const labelsSeparator = ";"

type wrapper struct {
	changeTime atomic.Int64
}

func newWrapper() *wrapper {
	w := &wrapper{}
	w.changeTime.Store(xtime.GetInaccurateUnixNano())

	return w
}

func (w *wrapper) update() {
	w.changeTime.Store(xtime.GetInaccurateUnixNano())
}

func (w *wrapper) isObsolete(holdDuration time.Duration) bool {
	return xtime.GetInaccurateUnixNano()-w.changeTime.Load() > holdDuration.Nanoseconds()
}

type CounterWrapper struct {
	*wrapper
	counter prometheus.Counter
}

func newCounterWrapper(counter prometheus.Counter) *CounterWrapper {
	return &CounterWrapper{
		counter: counter,
		wrapper: newWrapper(),
	}
}

func (cw *CounterWrapper) Inc() {
	cw.counter.Inc()
	cw.update()
}

func (cw *CounterWrapper) Add(v float64) {
	cw.counter.Add(v)
	cw.update()
}

type GaugeWrapper struct {
	*wrapper
	gauge prometheus.Gauge
}

func newGaugeWrapper(gauge prometheus.Gauge) *GaugeWrapper {
	return &GaugeWrapper{
		gauge:   gauge,
		wrapper: newWrapper(),
	}
}

func (gw *GaugeWrapper) Set(v float64) {
	gw.gauge.Set(v)
	gw.update()
}

func (gw *GaugeWrapper) Inc() {
	gw.gauge.Inc()
	gw.update()
}

func (gw *GaugeWrapper) Dec() {
	gw.gauge.Dec()
	gw.update()
}

func (gw *GaugeWrapper) Add(v float64) {
	gw.gauge.Add(v)
	gw.update()
}

func (gw *GaugeWrapper) Sub(v float64) {
	gw.gauge.Sub(v)
	gw.update()
}

type HistogramWrapper struct {
	*wrapper
	histogram prometheus.Histogram
}

func newHistogramWrapper(histogram prometheus.Histogram) *HistogramWrapper {
	return &HistogramWrapper{
		histogram: histogram,
		wrapper:   newWrapper(),
	}
}

func (hw *HistogramWrapper) Observe(v float64) {
	hw.histogram.Observe(v)
	hw.update()
}

type CounterVecWrapper struct {
	wrapperByLabels map[string]*CounterWrapper
	mu              *sync.Mutex

	vec *prometheus.CounterVec
}

func newCounterVecWrapper(cv *prometheus.CounterVec) *CounterVecWrapper {
	return &CounterVecWrapper{
		vec: cv,

		wrapperByLabels: make(map[string]*CounterWrapper),
		mu:              new(sync.Mutex),
	}
}

func (cvw *CounterVecWrapper) WithLabelValues(lvs ...string) *CounterWrapper {
	labels := strings.Join(lvs, labelsSeparator)

	cvw.mu.Lock()
	defer cvw.mu.Unlock()

	if hw, ok := cvw.wrapperByLabels[labels]; ok {
		return hw
	}

	c := cvw.vec.WithLabelValues(lvs...)
	cw := newCounterWrapper(c)
	cvw.wrapperByLabels[labels] = cw

	return cw
}

func (cvw *CounterVecWrapper) updateState(holdDuration time.Duration) {
	cvw.mu.Lock()
	defer cvw.mu.Unlock()

	for lvs, cw := range cvw.wrapperByLabels {
		if cw.isObsolete(holdDuration) {
			cvw.vec.DeleteLabelValues(strings.Split(lvs, labelsSeparator)...)
			delete(cvw.wrapperByLabels, lvs)
		}
	}
}

type GaugeVecWrapper struct {
	wrapperByLabels map[string]*GaugeWrapper
	mu              *sync.Mutex

	vec *prometheus.GaugeVec
}

func newGaugeVecWrapper(gv *prometheus.GaugeVec) *GaugeVecWrapper {
	return &GaugeVecWrapper{
		vec: gv,

		wrapperByLabels: make(map[string]*GaugeWrapper),
		mu:              new(sync.Mutex),
	}
}

func (gvw *GaugeVecWrapper) WithLabelValues(lvs ...string) *GaugeWrapper {
	labels := strings.Join(lvs, labelsSeparator)

	gvw.mu.Lock()
	defer gvw.mu.Unlock()

	if hw, ok := gvw.wrapperByLabels[labels]; ok {
		return hw
	}

	g := gvw.vec.WithLabelValues(lvs...)
	gw := newGaugeWrapper(g)
	gvw.wrapperByLabels[labels] = gw

	return gw
}

func (gvw *GaugeVecWrapper) updateState(holdDuration time.Duration) {
	gvw.mu.Lock()
	defer gvw.mu.Unlock()

	for lvs, gw := range gvw.wrapperByLabels {
		if gw.isObsolete(holdDuration) {
			gvw.vec.DeleteLabelValues(strings.Split(lvs, labelsSeparator)...)
			delete(gvw.wrapperByLabels, lvs)
		}
	}
}

type HistogramVecWrapper struct {
	wrapperByLabels map[string]*HistogramWrapper
	mu              *sync.Mutex

	vec *prometheus.HistogramVec
}

func newHistogramVecWrapper(hv *prometheus.HistogramVec) *HistogramVecWrapper {
	return &HistogramVecWrapper{
		vec: hv,

		wrapperByLabels: make(map[string]*HistogramWrapper),
		mu:              new(sync.Mutex),
	}
}

func (hvw *HistogramVecWrapper) WithLabelValues(lvs ...string) *HistogramWrapper {
	labels := strings.Join(lvs, labelsSeparator)

	hvw.mu.Lock()
	defer hvw.mu.Unlock()

	if hw, ok := hvw.wrapperByLabels[labels]; ok {
		return hw
	}

	h := hvw.vec.WithLabelValues(lvs...).(prometheus.Histogram)
	hw := newHistogramWrapper(h)
	hvw.wrapperByLabels[labels] = hw

	return hw
}

func (hvw *HistogramVecWrapper) updateState(holdDuration time.Duration) {
	hvw.mu.Lock()
	defer hvw.mu.Unlock()

	for lvs, hw := range hvw.wrapperByLabels {
		if hw.isObsolete(holdDuration) {
			hvw.vec.DeleteLabelValues(strings.Split(lvs, labelsSeparator)...)
			delete(hvw.wrapperByLabels, lvs)
		}
	}
}
