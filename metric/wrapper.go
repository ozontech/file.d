package metric

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/ozontech/file.d/xtime"
)

type (
	wrapper struct {
		changeTime time.Time
		active     bool
		labels     []string
		registrar  chan<- prometheus.Collector
	}

	CounterWrapper struct {
		wrapper
		counter prometheus.Counter
	}

	GaugeWrapper struct {
		wrapper
		gauge prometheus.Gauge
	}

	HistogramWrapper struct {
		wrapper
		histogram prometheus.Histogram
	}

	wrapperType interface {
		CounterWrapper | GaugeWrapper | HistogramWrapper
	}

	wrapperVec[T wrapperType] struct {
		elems     map[prometheus.Metric]*T
		mu        *sync.RWMutex
		registrar chan<- prometheus.Collector
	}

	CounterVecWrapper struct {
		wrapperVec[CounterWrapper]
		vec *prometheus.CounterVec
	}

	GaugeVecWrapper struct {
		wrapperVec[GaugeWrapper]
		vec *prometheus.GaugeVec
	}

	HistogramVecWrapper struct {
		wrapperVec[HistogramWrapper]
		vec *prometheus.HistogramVec
	}
)

func (c *CounterWrapper) Inc() {
	c.counter.Inc()
	c.update(c.counter)
}

func (c *CounterWrapper) Add(v float64) {
	c.counter.Add(v)
	c.update(c.counter)
}

func (g *GaugeWrapper) Set(v float64) {
	g.gauge.Set(v)
	g.update(g.gauge)
}

func (g *GaugeWrapper) Inc() {
	g.gauge.Inc()
	g.update(g.gauge)
}

func (g *GaugeWrapper) Dec() {
	g.gauge.Dec()
	g.update(g.gauge)
}

func (g *GaugeWrapper) Add(v float64) {
	g.gauge.Add(v)
	g.update(g.gauge)
}

func (g *GaugeWrapper) Sub(v float64) {
	g.gauge.Sub(v)
	g.update(g.gauge)
}

func (h *HistogramWrapper) Observe(v float64) {
	h.histogram.Observe(v)
	h.update(h.histogram)
}

func (cv *CounterVecWrapper) WithLabelValues(lvs ...string) *CounterWrapper {
	c := cv.vec.WithLabelValues(lvs...)
	if cw := cv.getElem(c); cw != nil {
		return cw
	}

	cw := newCounterWrapper(c, cv.registrar, lvs...)
	cv.setElem(c, cw)
	return cw
}

func (gv *GaugeVecWrapper) WithLabelValues(lvs ...string) *GaugeWrapper {
	g := gv.vec.WithLabelValues(lvs...)
	if gw := gv.getElem(g); gw != nil {
		return gw
	}

	gw := newGaugeWrapper(g, gv.registrar, lvs...)
	gv.setElem(g, gw)
	return gw
}

func (hv *HistogramVecWrapper) WithLabelValues(lvs ...string) *HistogramWrapper {
	h := hv.vec.WithLabelValues(lvs...).(prometheus.Histogram)
	if hw := hv.getElem(h); hw != nil {
		return hw
	}

	hw := newHistogramWrapper(h, hv.registrar, lvs...)
	hv.setElem(h, hw)
	return hw
}

func newWrapper(r chan<- prometheus.Collector, labels ...string) wrapper {
	w := wrapper{
		active:    true,
		registrar: r,
	}

	if labels != nil {
		w.labels = make([]string, len(labels))
		copy(w.labels, labels)
	}

	return w
}

func (w *wrapper) update(col prometheus.Collector) {
	w.changeTime = xtime.GetInaccurateTime()
	if !w.active {
		// Need re-register only metrics without labels.
		// MetricVecs have a shared metric-descriptor,
		// need to delete only metric with specified labels.
		if w.labels == nil {
			w.registrar <- col
		}
		w.active = true
	}
}

func (w *wrapper) isObsolete(holdDuration time.Duration) bool {
	return w.active && xtime.GetInaccurateTime().Sub(w.changeTime) > holdDuration
}

func newCounterWrapper(c prometheus.Counter, r chan<- prometheus.Collector, labels ...string) *CounterWrapper {
	return &CounterWrapper{
		counter: c,
		wrapper: newWrapper(r, labels...),
	}
}

func newGaugeWrapper(g prometheus.Gauge, r chan<- prometheus.Collector, labels ...string) *GaugeWrapper {
	return &GaugeWrapper{
		gauge:   g,
		wrapper: newWrapper(r, labels...),
	}
}

func newHistogramWrapper(h prometheus.Histogram, r chan<- prometheus.Collector, labels ...string) *HistogramWrapper {
	return &HistogramWrapper{
		histogram: h,
		wrapper:   newWrapper(r, labels...),
	}
}

func newWrapperVec[T wrapperType](r chan<- prometheus.Collector) wrapperVec[T] {
	return wrapperVec[T]{
		elems:     make(map[prometheus.Metric]*T),
		mu:        new(sync.RWMutex),
		registrar: r,
	}
}

func (wv *wrapperVec[T]) getElem(m prometheus.Metric) *T {
	wv.mu.RLock()
	defer wv.mu.RUnlock()
	return wv.elems[m]
}

func (wv *wrapperVec[T]) setElem(m prometheus.Metric, elem *T) {
	wv.mu.Lock()
	wv.elems[m] = elem
	wv.mu.Unlock()
}

func (wv *wrapperVec[T]) deleteElem(m prometheus.Metric) {
	wv.mu.Lock()
	delete(wv.elems, m)
	wv.mu.Unlock()
}

func newCounterVecWrapper(cv *prometheus.CounterVec, r chan<- prometheus.Collector) *CounterVecWrapper {
	return &CounterVecWrapper{
		vec:        cv,
		wrapperVec: newWrapperVec[CounterWrapper](r),
	}
}

func newGaugeVecWrapper(gv *prometheus.GaugeVec, r chan<- prometheus.Collector) *GaugeVecWrapper {
	return &GaugeVecWrapper{
		vec:        gv,
		wrapperVec: newWrapperVec[GaugeWrapper](r),
	}
}

func newHistogramVecWrapper(hv *prometheus.HistogramVec, r chan<- prometheus.Collector) *HistogramVecWrapper {
	return &HistogramVecWrapper{
		vec:        hv,
		wrapperVec: newWrapperVec[HistogramWrapper](r),
	}
}
