package throttle

import (
	"math"
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
	// need copy due to `limitDistributions` structure
	limitCopy := complexLimit{
		value:         limit.value,
		kind:          limit.kind,
		distributions: limit.distributions.copy(),
	}

	return &inMemoryLimiter{
		limit: limitCopy,
		buckets: newBuckets(
			cfg.bucketsCount,
			limitCopy.distributions.size()+1, // +1 because of default distribution
			cfg.bucketInterval,
		),

		nowFn: nowFn,
	}
}

func (l *inMemoryLimiter) sync() {}

func (l *inMemoryLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	// limit value fast check without races
	if atomic.LoadInt64(&l.limit.value) < 0 {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// If the limit is given distributions, then sharded buckets are used
	shard := 0
	distribution := 1.0
	if l.limit.distributions.size() > 0 {
		key := event.Root.Dig(l.limit.distributions.field...).AsString()
		shard, distribution = l.limit.distributions.get(key)

		// The shard index in the bucket matches the priority value index in priorities,
		// but is shifted by 1 because default priority has index 0.
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

	limit := int64(math.Round(distribution * float64(l.limit.value)))
	return l.buckets.get(index, shard) <= limit
}

func (l *inMemoryLimiter) lock() {
	l.mu.Lock()
}

func (l *inMemoryLimiter) unlock() {
	l.mu.Unlock()
}

func (l *inMemoryLimiter) getBucket(bucketIdx int) []int64 {
	return l.buckets.getAll(bucketIdx)
}

// Not thread safe - use lock&unlock methods!
func (l *inMemoryLimiter) updateBucket(bucketIdx, shardIdx int, value int64) {
	l.buckets.set(bucketIdx, shardIdx, value)
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
	l.mu.Lock()
	l.nowFn = fn
	l.mu.Unlock()
}
