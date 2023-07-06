package metric

import (
	"sync"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
)

const genTimeInterval = time.Second

var nowTime time.Time

type Holder struct {
	registry     *prom.Registry
	updateTime   time.Time
	holdInterval time.Duration
	metrics      metrics

	regChan     chan prom.Collector
	regChanStop chan struct{}

	genTimeTicker *time.Ticker
	genTimeStop   chan struct{}
}

func NewHolder(registry *prom.Registry, holdInterval time.Duration) *Holder {
	return &Holder{
		registry:     registry,
		holdInterval: holdInterval,
		metrics:      newMetrics(),

		regChan:     make(chan prom.Collector),
		regChanStop: make(chan struct{}),

		genTimeStop: make(chan struct{}),
	}
}

func (h *Holder) Start() {
	go h.genTime()
	go h.registerMetrics()
}

func (h *Holder) Stop() {
	h.regChanStop <- struct{}{}
	h.genTimeStop <- struct{}{}
}

func (h *Holder) Maintenance() {
	if time.Since(h.updateTime) < h.holdInterval {
		return
	}

	h.updateMetrics()
}

func (h *Holder) AddCounter(counter prom.Counter) *CounterWrapper {
	h.metrics.mu.RLock()
	cw, ok := h.metrics.counters[counter]
	h.metrics.mu.RUnlock()
	if ok {
		return cw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	cw, ok = h.metrics.counters[counter]
	if !ok {
		cw = newCounterWrapper(counter, h.regChan)
		h.metrics.counters[counter] = cw
	}

	return cw
}

func (h *Holder) AddCounterVec(counterVec *prom.CounterVec) *CounterVecWrapper {
	h.metrics.mu.RLock()
	cvw, ok := h.metrics.counterVecs[counterVec]
	h.metrics.mu.RUnlock()
	if ok {
		return cvw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	cvw, ok = h.metrics.counterVecs[counterVec]
	if !ok {
		cvw = newCounterVecWrapper(counterVec, h.regChan)
		h.metrics.counterVecs[counterVec] = cvw
	}

	return cvw
}

func (h *Holder) AddGauge(gauge prom.Gauge) *GaugeWrapper {
	h.metrics.mu.RLock()
	gw, ok := h.metrics.gauges[gauge]
	h.metrics.mu.RUnlock()
	if ok {
		return gw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	gw, ok = h.metrics.gauges[gauge]
	if !ok {
		gw = newGaugeWrapper(gauge, h.regChan)
		h.metrics.gauges[gauge] = gw
	}

	return gw
}

func (h *Holder) AddGaugeVec(gaugeVec *prom.GaugeVec) *GaugeVecWrapper {
	h.metrics.mu.RLock()
	gvw, ok := h.metrics.gaugeVecs[gaugeVec]
	h.metrics.mu.RUnlock()
	if ok {
		return gvw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	gvw, ok = h.metrics.gaugeVecs[gaugeVec]
	if !ok {
		gvw = newGaugeVecWrapper(gaugeVec, h.regChan)
		h.metrics.gaugeVecs[gaugeVec] = gvw
	}

	return gvw
}

func (h *Holder) AddHistogram(histogram prom.Histogram) *HistogramWrapper {
	h.metrics.mu.RLock()
	hw, ok := h.metrics.histograms[histogram]
	h.metrics.mu.RUnlock()
	if ok {
		return hw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	hw, ok = h.metrics.histograms[histogram]
	if !ok {
		hw = newHistogramWrapper(histogram, h.regChan)
		h.metrics.histograms[histogram] = hw
	}

	return hw
}

func (h *Holder) AddHistogramVec(histogramVec *prom.HistogramVec) *HistogramVecWrapper {
	h.metrics.mu.RLock()
	hvw, ok := h.metrics.histogramVecs[histogramVec]
	h.metrics.mu.RUnlock()
	if ok {
		return hvw
	}

	h.metrics.mu.Lock()
	defer h.metrics.mu.Unlock()
	hvw, ok = h.metrics.histogramVecs[histogramVec]
	if !ok {
		hvw = newHistogramVecWrapper(histogramVec, h.regChan)
		h.metrics.histogramVecs[histogramVec] = hvw
	}

	return hvw
}

type metrics struct {
	counters    map[prom.Counter]*CounterWrapper
	counterVecs map[*prom.CounterVec]*CounterVecWrapper

	gauges    map[prom.Gauge]*GaugeWrapper
	gaugeVecs map[*prom.GaugeVec]*GaugeVecWrapper

	histograms    map[prom.Histogram]*HistogramWrapper
	histogramVecs map[*prom.HistogramVec]*HistogramVecWrapper

	mu *sync.RWMutex
}

func newMetrics() metrics {
	return metrics{
		counters:      make(map[prom.Counter]*CounterWrapper),
		counterVecs:   make(map[*prom.CounterVec]*CounterVecWrapper),
		gauges:        make(map[prom.Gauge]*GaugeWrapper),
		gaugeVecs:     make(map[*prom.GaugeVec]*GaugeVecWrapper),
		histograms:    make(map[prom.Histogram]*HistogramWrapper),
		histogramVecs: make(map[*prom.HistogramVec]*HistogramVecWrapper),
		mu:            new(sync.RWMutex),
	}
}

// updateMetrics delete old metrics, that aren't in use since last update.
func (h *Holder) updateMetrics() {
	needDeleteMetric := func(wr *wrapper) bool {
		return wr.active && wr.changeTime.Before(h.updateTime)
	}

	updateMetric := func(col prom.Collector, wr *wrapper) {
		if needDeleteMetric(wr) {
			h.registry.Unregister(col)
			wr.active = false
		}
	}

	updateMetricVec := func(vec *prom.MetricVec, wr *wrapper) bool {
		if needDeleteMetric(wr) {
			vec.DeleteLabelValues(wr.labels...)
			wr.active = false
		}
		return !wr.active
	}

	for _, cw := range h.metrics.counters {
		updateMetric(cw.counter, &cw.wrapper)
	}

	for _, gw := range h.metrics.gauges {
		updateMetric(gw.gauge, &gw.wrapper)
	}

	for _, hw := range h.metrics.histograms {
		updateMetric(hw.histogram, &hw.wrapper)
	}

	for _, cvw := range h.metrics.counterVecs {
		for _, cw := range cvw.elems {
			if updateMetricVec(cvw.vec.MetricVec, &cw.wrapper) {
				cvw.deleteElem(cw.counter)
			}
		}
	}

	for _, gvw := range h.metrics.gaugeVecs {
		for _, gw := range gvw.elems {
			if updateMetricVec(gvw.vec.MetricVec, &gw.wrapper) {
				gvw.deleteElem(gw.gauge)
			}
		}
	}

	for _, hvw := range h.metrics.histogramVecs {
		for _, hw := range hvw.elems {
			if updateMetricVec(hvw.vec.MetricVec, &hw.wrapper) {
				hvw.deleteElem(hw.histogram)
			}
		}
	}

	h.updateTime = time.Now()
}

func (h *Holder) registerMetrics() {
	for {
		select {
		case col := <-h.regChan:
			h.registry.MustRegister(col)
		case <-h.regChanStop:
			return
		}
	}
}

func (h *Holder) genTime() {
	h.genTimeTicker = time.NewTicker(genTimeInterval)
	for {
		select {
		case <-h.genTimeTicker.C:
			nowTime = time.Now()
		case <-h.genTimeStop:
			h.genTimeTicker.Stop()
			return
		}
	}
}
