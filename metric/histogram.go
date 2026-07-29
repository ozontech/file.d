package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Histogram struct {
	*heldMetric[prometheus.Histogram]
}

func newHistogram(c prometheus.Histogram) *Histogram {
	return &Histogram{
		heldMetric: newHeldMetric(nil, c),
	}
}

//nolint:unused
func (h *Histogram) getHeldMetric() *heldMetric[prometheus.Histogram] {
	return h.heldMetric
}

func (h *Histogram) Observe(v float64) {
	h.metric.Observe(v)
	h.updateUsage()
}

type HistogramVec struct {
	store *heldMetricsStore[prometheus.Histogram, *Histogram]
	vec   *prometheus.HistogramVec
}

func newHistogramVec(hv *prometheus.HistogramVec, maxLabelValueLength int) *HistogramVec {
	return &HistogramVec{
		vec: hv,
		store: newHeldMetricsStore(
			maxLabelValueLength,
			func(hm *heldMetric[prometheus.Histogram]) *Histogram {
				return &Histogram{heldMetric: hm}
			},
		),
	}
}

func (hv *HistogramVec) WithLabelValues(lvs ...string) *Histogram {
	return hv.store.GetOrCreate(lvs, func(s ...string) prometheus.Histogram {
		return hv.vec.WithLabelValues(s...).(prometheus.Histogram)
	})
}

func (hv *HistogramVec) DeleteLabelValues(lvs ...string) bool {
	return hv.store.Delete(lvs, hv.vec)
}

func (hv *HistogramVec) DeleteOldMetrics(holdDuration time.Duration) {
	hv.store.DeleteOldMetrics(holdDuration, hv.vec)
}
