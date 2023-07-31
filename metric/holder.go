package metric

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const genTimeInterval = time.Second

var (
	nowTime   time.Time
	nowTimeMx = new(sync.RWMutex)
)

func setNowTime(t time.Time) {
	nowTimeMx.Lock()
	nowTime = t
	nowTimeMx.Unlock()
}

func getNowTime() time.Time {
	nowTimeMx.RLock()
	defer nowTimeMx.RUnlock()
	return nowTime
}

type Holder struct {
	registry     *prometheus.Registry
	holdDuration time.Duration
	metrics      metrics

	regChan chan prometheus.Collector

	genTimeTicker *time.Ticker
	genTimeStop   chan struct{}
}

func NewHolder(registry *prometheus.Registry, holdDuration time.Duration) *Holder {
	return &Holder{
		registry:     registry,
		holdDuration: holdDuration,
		metrics:      newMetrics(),

		regChan: make(chan prometheus.Collector),

		genTimeStop: make(chan struct{}),
	}
}

func (h *Holder) Start() {
	go h.genTime()
	go h.registerMetrics()
}

func (h *Holder) Stop() {
	close(h.regChan)
	h.genTimeStop <- struct{}{}
}

func (h *Holder) Maintenance() {
	h.updateMetrics()
}

func (h *Holder) AddCounter(counter prometheus.Counter) *CounterWrapper {
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

func (h *Holder) AddCounterVec(counterVec *prometheus.CounterVec) *CounterVecWrapper {
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

func (h *Holder) AddGauge(gauge prometheus.Gauge) *GaugeWrapper {
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

func (h *Holder) AddGaugeVec(gaugeVec *prometheus.GaugeVec) *GaugeVecWrapper {
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

func (h *Holder) AddHistogram(histogram prometheus.Histogram) *HistogramWrapper {
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

func (h *Holder) AddHistogramVec(histogramVec *prometheus.HistogramVec) *HistogramVecWrapper {
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
	counters    map[prometheus.Counter]*CounterWrapper
	counterVecs map[*prometheus.CounterVec]*CounterVecWrapper

	gauges    map[prometheus.Gauge]*GaugeWrapper
	gaugeVecs map[*prometheus.GaugeVec]*GaugeVecWrapper

	histograms    map[prometheus.Histogram]*HistogramWrapper
	histogramVecs map[*prometheus.HistogramVec]*HistogramVecWrapper

	mu *sync.RWMutex
}

func newMetrics() metrics {
	return metrics{
		counters:      make(map[prometheus.Counter]*CounterWrapper),
		counterVecs:   make(map[*prometheus.CounterVec]*CounterVecWrapper),
		gauges:        make(map[prometheus.Gauge]*GaugeWrapper),
		gaugeVecs:     make(map[*prometheus.GaugeVec]*GaugeVecWrapper),
		histograms:    make(map[prometheus.Histogram]*HistogramWrapper),
		histogramVecs: make(map[*prometheus.HistogramVec]*HistogramVecWrapper),
		mu:            new(sync.RWMutex),
	}
}

func (h *Holder) updateMetric(col prometheus.Collector, wr *wrapper) {
	if wr.isObsolete(h.holdDuration) {
		h.registry.Unregister(col)
		wr.active = false
	}
}

func updateMetricVec[T wrapperType](h *Holder, metricVec *prometheus.MetricVec, wrVec *wrapperVec[T], metric prometheus.Metric, wr *wrapper) {
	if wr.isObsolete(h.holdDuration) {
		metricVec.DeleteLabelValues(wr.labels...)
		wrVec.deleteElem(metric)
		wr.active = false
	}
}

// updateMetrics delete old metrics, that aren't in use since last update.
func (h *Holder) updateMetrics() {
	for _, cw := range h.metrics.counters {
		h.updateMetric(cw.counter, &cw.wrapper)
	}

	for _, gw := range h.metrics.gauges {
		h.updateMetric(gw.gauge, &gw.wrapper)
	}

	for _, hw := range h.metrics.histograms {
		h.updateMetric(hw.histogram, &hw.wrapper)
	}

	for _, cvw := range h.metrics.counterVecs {
		for _, cw := range cvw.elems {
			updateMetricVec(h, cvw.vec.MetricVec, &cvw.wrapperVec, cw.counter, &cw.wrapper)
		}
	}

	for _, gvw := range h.metrics.gaugeVecs {
		for _, gw := range gvw.elems {
			updateMetricVec(h, gvw.vec.MetricVec, &gvw.wrapperVec, gw.gauge, &gw.wrapper)
		}
	}

	for _, hvw := range h.metrics.histogramVecs {
		for _, hw := range hvw.elems {
			updateMetricVec(h, hvw.vec.MetricVec, &hvw.wrapperVec, hw.histogram, &hw.wrapper)
		}
	}
}

func (h *Holder) registerMetrics() {
	for col := range h.regChan {
		h.registry.MustRegister(col)
	}
}

func (h *Holder) genTime() {
	h.genTimeTicker = time.NewTicker(genTimeInterval)
	setNowTime(time.Now())
	for {
		select {
		case t := <-h.genTimeTicker.C:
			setNowTime(t)
		case <-h.genTimeStop:
			h.genTimeTicker.Stop()
			return
		}
	}
}
