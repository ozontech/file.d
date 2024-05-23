package throttle

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

type inMemoryLimiter struct {
	limit   complexLimit
	buckets buckets
	mu      sync.Mutex

	// nowFn is passed to create limiters and required for test purposes
	nowFn func() time.Time

	// metrics
	metricLabelsBuf   []string
	limitDistrMetrics *limitDistributionMetrics
}

// newInMemoryLimiter returns limiter instance.
func newInMemoryLimiter(
	cfg *limiterConfig,
	limit *complexLimit,
	limitDistrMetrics *limitDistributionMetrics,
	nowFn func() time.Time,
) *inMemoryLimiter {
	distSize := limit.distributions.size()

	l := &inMemoryLimiter{
		limit: complexLimit{
			value: limit.value,
			kind:  limit.kind,
		},
		buckets: newBuckets(
			cfg.bucketsCount,
			distSize+1, // +1 because of default distribution
			cfg.bucketInterval,
		),

		nowFn: nowFn,

		metricLabelsBuf:   make([]string, 0, len(limitDistrMetrics.CustomLabels)+1),
		limitDistrMetrics: limitDistrMetrics,
	}

	// need a copy due to possible runtime changes (sync with redis)
	if distSize > 0 {
		l.limit.distributions = limit.distributions.copy()
	}

	return l
}

func (l *inMemoryLimiter) sync() {}

func (l *inMemoryLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	limit := atomic.LoadInt64(&l.limit.value)

	// limit value fast check without races
	if limit < 0 {
		return true
	}

	l.lock()
	defer l.unlock()

	// If the limit is given with distribution, then distributed buckets are used
	distrIdx := 0
	distrFieldVal := ""
	if l.limit.distributions.isEnabled() {
		distrFieldVal = event.Root.Dig(l.limit.distributions.field...).AsString()
		distrIdx, limit = l.limit.distributions.getLimit(distrFieldVal)

		// The distribution index in the bucket matches the distribution value index in distributions,
		// but is shifted by 1 because default distribution has index 0.
		distrIdx++
	}

	id := l.rebuildBuckets(ts)
	index := id - l.buckets.getMinID()
	switch l.limit.kind {
	case "", limitKindCount:
		l.buckets.add(index, distrIdx, 1)
	case limitKindSize:
		l.buckets.add(index, distrIdx, int64(event.Size))
	default:
		logger.Fatalf("unknown type of the inMemoryLimiter: %q", l.limit.kind)
	}

	isAllowed := l.buckets.get(index, distrIdx) <= limit
	if !isAllowed && l.limit.distributions.isEnabled() {
		l.metricLabelsBuf = l.metricLabelsBuf[:0]

		l.metricLabelsBuf = append(l.metricLabelsBuf, distrFieldVal)
		for _, lbl := range l.limitDistrMetrics.CustomLabels {
			val := "not_set"
			node := event.Root.Dig(lbl)
			if node != nil {
				val = node.AsString()
			}
			l.metricLabelsBuf = append(l.metricLabelsBuf, val)
		}

		switch l.limit.kind {
		case "", limitKindCount:
			l.limitDistrMetrics.EventsCount.WithLabelValues(l.metricLabelsBuf...).Inc()
		case limitKindSize:
			l.limitDistrMetrics.EventsCount.WithLabelValues(l.metricLabelsBuf...).Add(float64(event.Size))
		}
	}

	return isAllowed
}

func (l *inMemoryLimiter) lock() {
	l.mu.Lock()
}

func (l *inMemoryLimiter) unlock() {
	l.mu.Unlock()
}

func (l *inMemoryLimiter) updateLimit(limit int64) {
	atomic.StoreInt64(&l.limit.value, limit)
}

func (l *inMemoryLimiter) updateDistribution(distribution limitDistributionCfg) error {
	if distribution.isEmpty() && l.limit.distributions.size() == 0 {
		return nil
	}
	ld, err := parseLimitDistribution(distribution, atomic.LoadInt64(&l.limit.value))
	if err != nil {
		return err
	}

	l.lock()
	defer l.unlock()

	// recreate buckets
	if l.limit.distributions.size() == 0 && ld.size() > 0 || l.limit.distributions.size() > 0 && ld.size() == 0 {
		l.buckets = newBuckets(
			l.buckets.getCount(),
			ld.size()+1, // +1 because of default distribution
			l.buckets.getInterval(),
		)
	}

	l.limit.distributions = ld
	return nil
}

func (l *inMemoryLimiter) getBucket(bucketIdx int, buf []int64) []int64 {
	return l.buckets.getAll(bucketIdx, buf)
}

// Not thread safe - use lock&unlock methods!
func (l *inMemoryLimiter) updateBucket(bucketIdx, distrIdx int, value int64) {
	l.buckets.set(bucketIdx, distrIdx, value)
}

// Not thread safe - use lock&unlock methods!
func (l *inMemoryLimiter) resetBucket(bucketIdx int) {
	l.buckets.reset(bucketIdx)
}

func (l *inMemoryLimiter) isBucketEmpty(bucketIdx int) bool {
	return l.buckets.isEmpty(bucketIdx)
}

// rebuildBuckets will rebuild buckets for given ts and returns actual bucket id
// Not thread safe - use lock&unlock methods!
func (l *inMemoryLimiter) rebuildBuckets(ts time.Time) int {
	return l.buckets.rebuild(l.nowFn(), ts)
}

// actualizeBucketIdx checks probable shift of buckets and returns actual bucket index and actuality
func (l *inMemoryLimiter) actualizeBucketIdx(maxID, bucketIdx int) (int, bool) {
	return l.buckets.actualizeIndex(maxID, bucketIdx)
}

func (l *inMemoryLimiter) bucketsCount() int {
	return l.buckets.getCount()
}

func (l *inMemoryLimiter) bucketsInterval() time.Duration {
	return l.buckets.getInterval()
}

func (l *inMemoryLimiter) bucketsMinID() int {
	return l.buckets.getMinID()
}

func (l *inMemoryLimiter) setNowFn(fn func() time.Time) {
	l.lock()
	l.nowFn = fn
	l.unlock()
}
