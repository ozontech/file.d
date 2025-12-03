package metric

import (
	"time"
)

type metricVec interface {
	DeleteOldMetrics(holdDuration time.Duration)
}

type Holder struct {
	holdDuration time.Duration
	heldMetrics  []metricVec
}

// NewHolder returns new metric holder. The holdDuration must be more than 1m.
func NewHolder(holdDuration time.Duration) *Holder {
	if holdDuration < time.Minute {
		panic("hold duration must be greater than 1m")
	}
	return &Holder{
		holdDuration: holdDuration,
		heldMetrics:  make([]metricVec, 0),
	}
}

func (h *Holder) Maintenance() {
	h.DeleteOldMetrics()
}

func (h *Holder) AddMetricVec(mv metricVec) {
	h.heldMetrics = append(h.heldMetrics, mv)
}

// DeleteOldMetrics delete old metric labels, that aren't in use since last update.
func (h *Holder) DeleteOldMetrics() {
	for i := range h.heldMetrics {
		h.heldMetrics[i].DeleteOldMetrics(h.holdDuration)
	}
}
