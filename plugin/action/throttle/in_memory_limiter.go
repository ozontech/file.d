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
}

// newInMemoryLimiter returns limiter instance.
func newInMemoryLimiter(cfg *limiterConfig, limit *complexLimit, nowFn func() time.Time) *inMemoryLimiter {
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
	}

	// need a copy due to possible runtime changes (sync with redis)
	if distSize > 0 {
		l.limit.distributions = limit.distributions.copy()
	}

	return l
}

func (l *inMemoryLimiter) sync() {}

func (l *inMemoryLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	// limit value fast check without races
	if atomic.LoadInt64(&l.limit.value) < 0 {
		return true
	}

	l.lock()
	defer l.unlock()

	// If the limit is given distributions, then sharded buckets are used
	shard := 0
	limit := l.limit.value
	if l.limit.distributions.isEnabled() {
		key := event.Root.Dig(l.limit.distributions.field...).AsString()
		shard, limit = l.limit.distributions.getLimit(key)

		// The shard index in the bucket matches the distribution value index in distributions,
		// but is shifted by 1 because default distribution has index 0.
		shard++
	}

	id := l.rebuildBuckets(ts)
	index := id - l.buckets.getMinID()
	switch l.limit.kind {
	case "", "count":
		l.buckets.add(index, shard, 1)
	case "size":
		l.buckets.add(index, shard, int64(event.Size))
	default:
		logger.Fatalf("unknown type of the inMemoryLimiter: %q", l.limit.kind)
	}

	return l.buckets.get(index, shard) <= limit
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
	l.limit.distributions = ld
	l.unlock()
	return nil
}

func (l *inMemoryLimiter) getBucket(bucketIdx int, buf []int64) []int64 {
	return l.buckets.getAll(bucketIdx, buf)
}

// Not thread safe - use lock&unlock methods!
func (l *inMemoryLimiter) updateBucket(bucketIdx, shardIdx int, value int64) {
	l.buckets.set(bucketIdx, shardIdx, value)
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
