package metric

import (
	"time"
)

type heldMetricVec interface {
	DeleteOldMetrics(holdDuration time.Duration)
}

type Holder struct {
	holdDuration              time.Duration
	heldMetrics               []heldMetricVec
	metricMaxLabelValueLength int
}

// NewHolder returns new metric holder. The holdDuration must be more than 1m.
func NewHolder(holdDuration time.Duration, metricMaxLabelValueLength int) *Holder {
	if holdDuration < time.Minute {
		panic("hold duration must be greater than 1m")
	}
	return &Holder{
		holdDuration:              holdDuration,
		heldMetrics:               make([]heldMetricVec, 0),
		metricMaxLabelValueLength: metricMaxLabelValueLength,
	}
}

func (h *Holder) maintenance() {
	h.deleteOldMetrics()
}

func (h *Holder) addMetricVec(mv heldMetricVec) {
	h.heldMetrics = append(h.heldMetrics, mv)
}

// DeleteOldMetrics delete old metric labels, that aren't in use since last update.
func (h *Holder) deleteOldMetrics() {
	for i := range h.heldMetrics {
		h.heldMetrics[i].DeleteOldMetrics(h.holdDuration)
	}
}
