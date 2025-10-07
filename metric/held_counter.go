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
	store          *heldMetricsStore[prometheus.Counter]
	vec            *prometheus.CounterVec
	maxLabelLength int
}

func NewHeldCounterVec(cv *prometheus.CounterVec, maxLabelLength int) HeldCounterVec {
	return HeldCounterVec{
		vec:            cv,
		store:          newHeldMetricsStore[prometheus.Counter](),
		maxLabelLength: maxLabelLength,
	}
}

func (h HeldCounterVec) WithLabelValues(lvs ...string) HeldCounter {
	TruncateLabels(lvs, h.maxLabelLength)

	return HeldCounter{
		heldMetric: h.store.GetOrCreate(lvs, h.vec.WithLabelValues),
	}
}

func (h HeldCounterVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}

func TruncateLabels(lvs []string, maxLabelLength int) {
	for i, label := range lvs {
		if len(label) > maxLabelLength {
			lvs[i] = label[:maxLabelLength]
		}
	}
}
