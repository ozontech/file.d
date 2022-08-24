package throttle

import (
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

type inMemoryLimiter struct {
	limit       complexLimit // threshold and type of an inMemoryLimiter
	bucketCount int
	buckets     []int64
	interval    time.Duration // bucket interval
	minID       int           // minimum bucket id
	maxID       int           // max bucket id
	mu          sync.Mutex
}

// NewInMemoryLimiter returns limiter instance.
func NewInMemoryLimiter(interval time.Duration, bucketCount int, limit complexLimit) *inMemoryLimiter {
	return &inMemoryLimiter{
		interval:    interval,
		bucketCount: bucketCount,
		limit:       limit,

		buckets: make([]int64, bucketCount),
	}
}

func (l *inMemoryLimiter) sync() {

}

func (l *inMemoryLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	id := l.rebuildBuckets(ts)
	index := id - l.minID
	switch l.limit.kind {
	default:
		logger.Fatalf("unknown type of the inMemoryLimiter: %q", l.limit.kind)
	case "", "count":
		l.buckets[index]++
	case "size":
		l.buckets[index] += int64(event.Size)
	}

	return l.buckets[index] <= l.limit.value
}

// rebuildBuckets will rebuild buckets for given ts and returns actual bucket id
// Not thread safe - use external lock!
func (l *inMemoryLimiter) rebuildBuckets(ts time.Time) int {
	currentTs := time.Now()
	currentID := l.timeToBucketID(currentTs)
	if l.minID == 0 {
		// min id weren't set yet. It MUST be extracted from currentTs, because ts from event can be invalid (e.g. from 1970 or 2077 year)
		l.maxID = l.timeToBucketID(currentTs)
		l.minID = l.maxID - l.bucketCount + 1
	}
	maxID := l.minID + len(l.buckets) - 1

	// currentBucket exceed maxID. Create actual buckets
	if currentID > maxID {
		n := currentID - maxID
		// add new buckets
		for i := 0; i < n; i++ {
			l.buckets = append(l.buckets, 0)
		}
		// remove old buckets
		l.buckets = l.buckets[n:]
		// update min buckets
		l.minID += n
		l.maxID = currentID
	}
	id := l.timeToBucketID(ts)

	// events from past or future goes to lastest backet
	if id < l.minID || id > maxID {
		id = maxID
	}
	return id
}

// timeToBucketID converts time to bucketID.
func (l *inMemoryLimiter) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / l.interval.Nanoseconds())
}
