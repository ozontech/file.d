package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type HeldHistogram struct {
	*heldMetric[prometheus.Histogram]
}

func (h HeldHistogram) Observe(v float64) {
	h.metric.Observe(v)
	h.updateUsage()
}

type HeldHistogramVec struct {
	store          *heldMetricsStore[prometheus.Histogram]
	vec            *prometheus.HistogramVec
	maxLabelLength int
}

func NewHeldHistogramVec(hv *prometheus.HistogramVec, maxLabelLength int) HeldHistogramVec {
	return HeldHistogramVec{
		vec:            hv,
		store:          newHeldMetricsStore[prometheus.Histogram](),
		maxLabelLength: maxLabelLength,
	}
}

func (h HeldHistogramVec) WithLabelValues(lvs ...string) HeldHistogram {
	truncateLabels(lvs, h.maxLabelLength)

	return HeldHistogram{
		heldMetric: h.store.GetOrCreate(lvs, func(s ...string) prometheus.Histogram {
			return h.vec.WithLabelValues(s...).(prometheus.Histogram)
		}),
	}
}

func (h HeldHistogramVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}
