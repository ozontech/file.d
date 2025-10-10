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
	store                     *heldMetricsStore[prometheus.Histogram]
	vec                       *prometheus.HistogramVec
	metricMaxLabelValueLength int
}

func NewHeldHistogramVec(hv *prometheus.HistogramVec, metricMaxLabelValueLength int) HeldHistogramVec {
	return HeldHistogramVec{
		vec:                       hv,
		store:                     newHeldMetricsStore[prometheus.Histogram](),
		metricMaxLabelValueLength: metricMaxLabelValueLength,
	}
}

func (h HeldHistogramVec) WithLabelValues(lvs ...string) HeldHistogram {
	TruncateLabels(lvs, h.metricMaxLabelValueLength)

	return HeldHistogram{
		heldMetric: h.store.GetOrCreate(lvs, func(s ...string) prometheus.Histogram {
			return h.vec.WithLabelValues(s...).(prometheus.Histogram)
		}),
	}
}

func (h HeldHistogramVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}
