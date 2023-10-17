package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type heldMetricVec interface {
	update(holdDuration time.Duration)
}

type Holder struct {
	holdDuration time.Duration
	heldMetrics  []heldMetricVec
}

func NewHolder(holdDuration time.Duration) *Holder {
	return &Holder{
		holdDuration: holdDuration,
		heldMetrics:  make([]heldMetricVec, 0),
	}
}

func (h *Holder) Maintenance() {
	h.updateMetrics()
}

func (h *Holder) AddCounterVec(counterVec *prometheus.CounterVec) *HeldCounterVec {
	hcv := newHeldCounterVec(counterVec)
	h.heldMetrics = append(h.heldMetrics, hcv)
	return hcv
}

func (h *Holder) AddGaugeVec(gaugeVec *prometheus.GaugeVec) *HeldGaugeVec {
	hgv := newHeldGaugeVec(gaugeVec)
	h.heldMetrics = append(h.heldMetrics, hgv)
	return hgv
}

func (h *Holder) AddHistogramVec(histogramVec *prometheus.HistogramVec) *HeldHistogramVec {
	hhv := newHeldHistogramVec(histogramVec)
	h.heldMetrics = append(h.heldMetrics, hhv)
	return hhv
}

// updateMetrics delete old metrics, that aren't in use since last update.
func (h *Holder) updateMetrics() {
	for i := range h.heldMetrics {
		h.heldMetrics[i].update(h.holdDuration)
	}
}
