package metric

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

type Counter struct {
	*heldMetric[prometheus.Counter]
}

func newCounter(c prometheus.Counter) *Counter {
	return &Counter{
		heldMetric: newHeldMetric(nil, c),
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

// should only be used in tests
func (c *Counter) ToFloat64() float64 {
	return testutil.ToFloat64(c.metric)
}

type CounterVec struct {
	store *heldMetricsStore[prometheus.Counter]
	vec   *prometheus.CounterVec
}

func newCounterVec(cv *prometheus.CounterVec, maxLabelValueLength int) *CounterVec {
	return &CounterVec{
		vec:   cv,
		store: newHeldMetricsStore[prometheus.Counter](maxLabelValueLength),
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
