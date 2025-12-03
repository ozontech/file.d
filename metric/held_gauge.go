package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Gauge struct {
	*heldMetric[prometheus.Gauge]
}

func NewGauge(c prometheus.Gauge) *Gauge {
	return &Gauge{
		heldMetric: newHeldMetric([]string{}, c),
	}
}

func (g *Gauge) Set(v float64) {
	g.metric.Set(v)
	g.updateUsage()
}

func (g *Gauge) Inc() {
	g.metric.Inc()
	g.updateUsage()
}

func (g *Gauge) Dec() {
	g.metric.Dec()
	g.updateUsage()
}

func (g *Gauge) Add(v float64) {
	g.metric.Add(v)
	g.updateUsage()
}

func (g *Gauge) Sub(v float64) {
	g.metric.Sub(v)
	g.updateUsage()
}

type GaugeVec struct {
	store *heldMetricsStore[prometheus.Gauge]
	vec   *prometheus.GaugeVec
}

func NewGaugeVec(gv *prometheus.GaugeVec) *GaugeVec {
	return &GaugeVec{
		vec:   gv,
		store: newHeldMetricsStore[prometheus.Gauge](),
	}
}

func (gv *GaugeVec) WithLabelValues(lvs ...string) *Gauge {
	return &Gauge{
		heldMetric: gv.store.GetOrCreate(lvs, gv.vec.WithLabelValues),
	}
}

func (cv *GaugeVec) DeleteLabelValues(lvs ...string) bool {
	return cv.store.Delete(lvs, cv.vec)
}

func (gv *GaugeVec) DeleteOldMetrics(holdDuration time.Duration) {
	gv.store.DeleteOldMetrics(holdDuration, gv.vec)
}
