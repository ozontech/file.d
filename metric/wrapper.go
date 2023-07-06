package metric

import (
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
)

type (
	wrapper struct {
		changeTime time.Time
		active     bool
		labels     []string
		registrar  chan<- prom.Collector
	}

	CounterWrapper struct {
		wrapper
		counter prom.Counter
	}

	GaugeWrapper struct {
		wrapper
		gauge prom.Gauge
	}

	HistogramWrapper struct {
		wrapper
		histogram prom.Histogram
	}

	wrapperType interface {
		CounterWrapper | GaugeWrapper | HistogramWrapper
	}

	wrapperVec[T wrapperType] struct {
		elems     map[prom.Metric]*T
		mu        *sync.RWMutex
		registrar chan<- prom.Collector
	}

	CounterVecWrapper struct {
		wrapperVec[CounterWrapper]
		vec *prom.CounterVec
	}

	GaugeVecWrapper struct {
		wrapperVec[GaugeWrapper]
		vec *prom.GaugeVec
	}

	HistogramVecWrapper struct {
		wrapperVec[HistogramWrapper]
		vec *prom.HistogramVec
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
	h := hv.vec.WithLabelValues(lvs...).(prom.Histogram)
	if hw := hv.getElem(h); hw != nil {
		return hw
	}

	hw := newHistogramWrapper(h, hv.registrar, lvs...)
	hv.setElem(h, hw)
	return hw
}

func newWrapper(r chan<- prom.Collector, labels ...string) wrapper {
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

func (w *wrapper) update(col prom.Collector) {
	w.changeTime = nowTime
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

func newCounterWrapper(c prom.Counter, r chan<- prom.Collector, labels ...string) *CounterWrapper {
	return &CounterWrapper{
		counter: c,
		wrapper: newWrapper(r, labels...),
	}
}

func newGaugeWrapper(g prom.Gauge, r chan<- prom.Collector, labels ...string) *GaugeWrapper {
	return &GaugeWrapper{
		gauge:   g,
		wrapper: newWrapper(r, labels...),
	}
}

func newHistogramWrapper(h prom.Histogram, r chan<- prom.Collector, labels ...string) *HistogramWrapper {
	return &HistogramWrapper{
		histogram: h,
		wrapper:   newWrapper(r, labels...),
	}
}

func newWrapperVec[T wrapperType](r chan<- prom.Collector) wrapperVec[T] {
	return wrapperVec[T]{
		elems:     make(map[prom.Metric]*T),
		mu:        new(sync.RWMutex),
		registrar: r,
	}
}

func (wv *wrapperVec[T]) getElem(m prom.Metric) *T {
	wv.mu.RLock()
	defer wv.mu.RUnlock()
	return wv.elems[m]
}

func (wv *wrapperVec[T]) setElem(m prom.Metric, elem *T) {
	wv.mu.Lock()
	wv.elems[m] = elem
	wv.mu.Unlock()
}

func (wv *wrapperVec[T]) deleteElem(m prom.Metric) {
	wv.mu.Lock()
	delete(wv.elems, m)
	wv.mu.Unlock()
}

func newCounterVecWrapper(cv *prom.CounterVec, r chan<- prom.Collector) *CounterVecWrapper {
	return &CounterVecWrapper{
		vec:        cv,
		wrapperVec: newWrapperVec[CounterWrapper](r),
	}
}

func newGaugeVecWrapper(gv *prom.GaugeVec, r chan<- prom.Collector) *GaugeVecWrapper {
	return &GaugeVecWrapper{
		vec:        gv,
		wrapperVec: newWrapperVec[GaugeWrapper](r),
	}
}

func newHistogramVecWrapper(hv *prom.HistogramVec, r chan<- prom.Collector) *HistogramVecWrapper {
	return &HistogramVecWrapper{
		vec:        hv,
		wrapperVec: newWrapperVec[HistogramWrapper](r),
	}
}
