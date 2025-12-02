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

func (h HeldCounter) Get() prometheus.Counter {
	return h.metric
}

type HeldCounterVec struct {
	store                     *heldMetricsStore[prometheus.Counter]
	vec                       *prometheus.CounterVec
	metricMaxLabelValueLength int
}

func NewHeldCounterVec(cv *prometheus.CounterVec, metricMaxLabelValueLength int) HeldCounterVec {
	return HeldCounterVec{
		vec:                       cv,
		store:                     newHeldMetricsStore[prometheus.Counter](),
		metricMaxLabelValueLength: metricMaxLabelValueLength,
	}
}

func (h HeldCounterVec) WithLabelValues(lvs ...string) HeldCounter {
	TruncateLabels(lvs, h.metricMaxLabelValueLength)

	return HeldCounter{
		heldMetric: h.store.GetOrCreate(lvs, h.vec.WithLabelValues),
	}
}

func (h HeldCounterVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}

func (h HeldCounterVec) GetVec() *prometheus.CounterVec {
	return h.vec
}

func TruncateLabels(lvs []string, metricMaxLabelValueLength int) {
	if metricMaxLabelValueLength == 0 {
		return
	}

	for i, label := range lvs {
		if len(label) > metricMaxLabelValueLength {
			lvs[i] = label[:metricMaxLabelValueLength]
		}
	}
}
