package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Histogram struct {
	*heldMetric[prometheus.Histogram]
}

func NewHistogram(c prometheus.Histogram) *Histogram {
	return &Histogram{
		heldMetric: newHeldMetric([]string{}, c),
	}
}

func (h *Histogram) Observe(v float64) {
	h.metric.Observe(v)
	h.updateUsage()
}

type HistogramVec struct {
	store *heldMetricsStore[prometheus.Histogram]
	vec   *prometheus.HistogramVec
}

func NewHistogramVec(hv *prometheus.HistogramVec) *HistogramVec {
	return &HistogramVec{
		vec:   hv,
		store: newHeldMetricsStore[prometheus.Histogram](),
	}
}

func (hv *HistogramVec) WithLabelValues(lvs ...string) *Histogram {
	return &Histogram{
		heldMetric: hv.store.GetOrCreate(lvs, func(s ...string) prometheus.Histogram {
			return hv.vec.WithLabelValues(s...).(prometheus.Histogram)
		}),
	}
}

func (cv *HistogramVec) DeleteLabelValues(lvs ...string) bool {
	return cv.store.Delete(lvs, cv.vec)
}

func (hv *HistogramVec) DeleteOldMetrics(holdDuration time.Duration) {
	hv.store.DeleteOldMetrics(holdDuration, hv.vec)
}
