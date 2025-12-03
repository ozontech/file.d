package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Counter struct {
	*heldMetric[prometheus.Counter]
}

func NewCounter(c prometheus.Counter) *Counter {
	return &Counter{
		heldMetric: newHeldMetric([]string{}, c),
	}
}

func (c *Counter) Inc() {
	c.metric.Inc()
	c.updateUsage()
}

func (c *Counter) Add(v float64) {
	c.metric.Add(v)
	c.updateUsage()
}

type CounterVec struct {
	store *heldMetricsStore[prometheus.Counter]
	vec   *prometheus.CounterVec
}

func NewCounterVec(cv *prometheus.CounterVec) *CounterVec {
	return &CounterVec{
		vec:   cv,
		store: newHeldMetricsStore[prometheus.Counter](),
	}
}

func (cv *CounterVec) WithLabelValues(lvs ...string) *Counter {
	return &Counter{
		heldMetric: cv.store.GetOrCreate(lvs, cv.vec.WithLabelValues),
	}
}

func (cv *CounterVec) DeleteLabelValues(lvs ...string) bool {
	return cv.store.Delete(lvs, cv.vec)
}

func (cv *CounterVec) DeleteOldMetrics(holdDuration time.Duration) {
	cv.store.DeleteOldMetrics(holdDuration, cv.vec)
}
