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

func (g *Gauge) getHeldMetric() *heldMetric[prometheus.Gauge] {
	return g.heldMetric
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
func (g *Gauge) ToFloat64() float64 {
	return testutil.ToFloat64(g.metric)
}

type GaugeVec struct {
	store *heldMetricsStore[prometheus.Gauge, *Gauge]
	vec   *prometheus.GaugeVec
}

func newGaugeVec(gv *prometheus.GaugeVec, maxLabelValueLength int) *GaugeVec {
	return &GaugeVec{
		vec: gv,
		store: newHeldMetricsStore[prometheus.Gauge, *Gauge](
			maxLabelValueLength,
			func(hm *heldMetric[prometheus.Gauge]) *Gauge {
				return &Gauge{heldMetric: hm}
			},
		),
	}
}

func (gv *GaugeVec) WithLabelValues(lvs ...string) *Gauge {
	return gv.store.GetOrCreate(lvs, gv.vec.WithLabelValues)
}

func (gv *GaugeVec) DeleteLabelValues(lvs ...string) bool {
	return gv.store.Delete(lvs, gv.vec)
}

func (gv *GaugeVec) DeleteOldMetrics(holdDuration time.Duration) {
	gv.store.DeleteOldMetrics(holdDuration, gv.vec)
}
