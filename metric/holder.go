package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type heldMetricVec interface {
	DeleteOldMetrics(holdDuration time.Duration)
}

type Holder struct {
	holdDuration time.Duration
	heldMetrics  []heldMetricVec
}

// NewHolder returns new metric holder. The holdDuration must be more than 1m.
func NewHolder(holdDuration time.Duration) *Holder {
	if holdDuration < time.Minute {
		panic("hold duration must be greater than 1m")
	}
	return &Holder{
		holdDuration: holdDuration,
		heldMetrics:  make([]heldMetricVec, 0),
	}
}

func (h *Holder) Maintenance() {
	h.DeleteOldMetrics()
}

func (h *Holder) AddCounterVec(counterVec *prometheus.CounterVec) HeldCounterVec {
	hcv := NewHeldCounterVec(counterVec)
	h.heldMetrics = append(h.heldMetrics, hcv)
	return hcv
}

func (h *Holder) AddGaugeVec(gaugeVec *prometheus.GaugeVec) HeldGaugeVec {
	hgv := NewHeldGaugeVec(gaugeVec)
	h.heldMetrics = append(h.heldMetrics, hgv)
	return hgv
}

func (h *Holder) AddHistogramVec(histogramVec *prometheus.HistogramVec) HeldHistogramVec {
	hhv := NewHeldHistogramVec(histogramVec)
	h.heldMetrics = append(h.heldMetrics, hhv)
	return hhv
}

// DeleteOldMetrics delete old metric labels, that aren't in use since last update.
func (h *Holder) DeleteOldMetrics() {
	for i := range h.heldMetrics {
		h.heldMetrics[i].DeleteOldMetrics(h.holdDuration)
	}
}
