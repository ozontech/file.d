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
	mu          sync.Mutex
}

func NewInMemoryLimiter(interval time.Duration, bucketCount int, limit complexLimit) *inMemoryLimiter {
	return &inMemoryLimiter{
		interval:    interval,
		bucketCount: bucketCount,
		limit:       limit,

		buckets: make([]int64, bucketCount),
	}
}

func (l *inMemoryLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	id := l.rebuildBuckets(ts)

	index := id - l.minID
	switch l.limit.kind {
	default:
		logger.Fatalf("unknown type of the inMemoryLimiter: %q", l.limit.kind)
	case "":
		fallthrough
	case "count":
		l.buckets[index]++
	case "size":
		l.buckets[index] += int64(event.Size)
	}

	return l.buckets[index] <= l.limit.value
}

// rebuildBuckets will rebuild buckets for given ts and returns actual bucket id
// Not thread safe - use external lock!
func (l *inMemoryLimiter) rebuildBuckets(ts time.Time) int {
	if l.minID == 0 {
		l.minID = l.timeToBucketID(ts) - l.bucketCount + 1
	}
	maxID := l.minID + len(l.buckets) - 1
	id := l.timeToBucketID(ts)

	// inMemoryLimiter doesn't track that bucket anymore.
	if id < l.minID {
		return 0
	}

	// event from a new bucket, add N new buckets
	if id > maxID {
		n := id - maxID
		for i := 0; i < n; i++ {
			l.buckets = append(l.buckets, 0)
		}
		// remove old ones
		l.buckets = l.buckets[n:]
		// and set new min index
		l.minID += n
	}
	return id
}

// timeToBucketID converts time to bucketID.
func (l *inMemoryLimiter) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / l.interval.Nanoseconds())
}
