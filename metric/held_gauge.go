package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type HeldGauge struct {
	*heldMetric[prometheus.Gauge]
}

func (h HeldGauge) Set(v float64) {
	h.metric.Set(v)
	h.updateUsage()
}

func (h HeldGauge) Inc() {
	h.metric.Inc()
	h.updateUsage()
}

func (h HeldGauge) Dec() {
	h.metric.Dec()
	h.updateUsage()
}

func (h HeldGauge) Add(v float64) {
	h.metric.Add(v)
	h.updateUsage()
}

func (h HeldGauge) Sub(v float64) {
	h.metric.Sub(v)
	h.updateUsage()
}

type HeldGaugeVec struct {
	store *heldMetricsStore[prometheus.Gauge]
	vec   *prometheus.GaugeVec
}

func NewHeldGaugeVec(gv *prometheus.GaugeVec) HeldGaugeVec {
	return HeldGaugeVec{
		vec:   gv,
		store: newHeldMetricsStore[prometheus.Gauge](),
	}
}

func (h HeldGaugeVec) WithLabelValues(lvs ...string) HeldGauge {
	return HeldGauge{
		heldMetric: h.store.GetOrCreate(lvs, h.vec.WithLabelValues),
	}
}

func (h HeldGaugeVec) DeleteOldMetrics(holdDuration time.Duration) {
	h.store.DeleteOldMetrics(holdDuration, h.vec)
}
