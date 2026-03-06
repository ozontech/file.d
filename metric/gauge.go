package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type Gauge struct {
	*heldMetric[prometheus.Gauge]
}

func newGauge(c prometheus.Gauge) *Gauge {
	return &Gauge{
		heldMetric: newHeldMetric(nil, c),
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

// should only be used in tests
func (c *Gauge) ToFloat64() float64 {
	return testutil.ToFloat64(c.metric)
}

type GaugeVec struct {
	store *heldMetricsStore[prometheus.Gauge]
	vec   *prometheus.GaugeVec
}

func newGaugeVec(gv *prometheus.GaugeVec, maxLabelValueLength int) *GaugeVec {
	return &GaugeVec{
		vec:   gv,
		store: newHeldMetricsStore[prometheus.Gauge](maxLabelValueLength),
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
