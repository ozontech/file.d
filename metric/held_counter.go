package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type HeldCounter struct {
	*heldMetric[prometheus.Counter]
}

func (h HeldCounter) Inc() {
	h.metric.Inc()
	h.updateUsage()
}

func (h HeldCounter) Add(v float64) {
	h.metric.Add(v)
	h.updateUsage()
}

type HeldCounterVec struct {
	store *heldMetricsStore[prometheus.Counter]
	vec   *prometheus.CounterVec
}

func NewHeldCounterVec(cv *prometheus.CounterVec) HeldCounterVec {
	return HeldCounterVec{
		vec:   cv,
		store: newHeldMetricsStore[prometheus.Counter](),
	}
}

func (h HeldCounterVec) WithLabelValues(lvs ...string) HeldCounter {
	return HeldCounter{
		heldMetric: h.store.GetOrCreate(lvs, h.vec.WithLabelValues),
	}
}

func (h HeldCounterVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}
