package throttle

import (
	"fmt"
	"io"
	"time"
)

type limiter struct {
	interval    time.Duration // bucket interval
	bucketCount int
	limit       int64 // maximum number of events per bucket

	buckets []int64
	minID   int // minimum bucket id
}

func NewLimiter(interval time.Duration, bucketCount int, limit int64) *limiter {
	return &limiter{
		interval:    interval,
		bucketCount: bucketCount,
		limit:       limit,

		buckets: make([]int64, bucketCount),
	}
}

// isAllowed returns TRUE if event is allowed to be processed.
func (l *limiter) isAllowed(ts time.Time) bool {
	if l.minID == 0 {
		l.minID = l.timeToBucketID(ts) - l.bucketCount + 1
	}
	maxID := l.minID + len(l.buckets) - 1

	id := l.timeToBucketID(ts)

	// limiter doesn't track that bucket anymore.
	if id < l.minID {
		return false
	}

	// event from new bucket, add N new buckets
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

	index := id - l.minID
	l.buckets[index]++

	return l.buckets[index] <= l.limit
}

// WriteStatus writes text based status into Writer.
func (l *limiter) writeStatus(w io.Writer) error {
	for i, value := range l.buckets {
		_, _ = fmt.Fprintf(w, "#%s: ", l.bucketIDToTime(i+l.minID))
		progress(w, value, l.limit, 20)
		_, _ = fmt.Fprintf(w, " %d/%d\n", value, l.limit)
	}
	return nil
}

func progress(w io.Writer, current, limit, max int64) {
	p := float64(current) / float64(limit) * float64(max)

	_, _ = fmt.Fprint(w, "[")
	for i := int64(0); i < max; i++ {
		if i < int64(p) {
			_, _ = fmt.Fprint(w, "#")
		} else {
			_, _ = fmt.Fprint(w, "_")
		}
	}
	_, _ = fmt.Fprint(w, "]")
}

// bucketIDToTime converts bucketID to time. This time is start of the bucket.
func (l *limiter) bucketIDToTime(id int) time.Time {
	nano := int64(id) * l.interval.Nanoseconds()
	return time.Unix(nano/100000000, nano%100000000)
}

// timeToBucketID converts time to bucketID.
func (l *limiter) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / l.interval.Nanoseconds())
}
