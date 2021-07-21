package throttle

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
)

type limiter interface {
	isAllowed(event *pipeline.Event, ts time.Time) bool
}

type redisLimiter struct {
	mu    sync.Mutex
	redis redisClient

	// <throttleField>_
	// will be used for bucket counter <throttleField>_<bucketID> and limit key <throttleField>_limit
	keyPrefix bytes.Buffer

	// contains values which will be used for incrementing remote bucket counter
	// buckets will be flushed after every sync to contain only increment value
	incrementLimiter *inMemoryLimiter

	// contains global values synced from redis
	totalLimiter *inMemoryLimiter
}

func NewRedisLimiter(
	redis redisClient,
	throttleFieldName, throttleFieldValue string,
	refreshInterval, bucketInterval time.Duration,
	bucketCount int,
	limit complexLimit,
) *redisLimiter {
	rl := &redisLimiter{
		redis:            redis,
		incrementLimiter: NewInMemoryLimiter(bucketInterval, bucketCount, limit),
		totalLimiter:     NewInMemoryLimiter(bucketInterval, bucketCount, limit),
	}

	rl.keyPrefix = bytes.Buffer{}
	rl.keyPrefix.WriteString(throttleFieldName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")

	go func() {
		for range time.Tick(refreshInterval) {
			rl.syncRedis()
		}
	}()

	return rl
}

func (l *redisLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// count increment from last sync
	ret := l.incrementLimiter.isAllowed(event, ts)
	if ret {
		// count global limiter if passed local limit
		ret = l.totalLimiter.isAllowed(event, ts)
	}

	return ret
}

func (l *redisLimiter) syncRedis() {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := time.Now()
	l.incrementLimiter.rebuildBuckets(n)
	l.totalLimiter.rebuildBuckets(n)

	id := l.totalLimiter.timeToBucketID(n)
	index := id - l.totalLimiter.minID

	// get actual bucket value
	keyPrefixLen := l.keyPrefix.Len()
	l.keyPrefix.WriteString(strconv.Itoa(id))

	// global bucket key
	key := l.keyPrefix.String()

	_ = l.redis.Watch(func(tx *redis.Tx) error {
		// inc global counter
		intCmd := tx.IncrBy(key, l.incrementLimiter.buckets[index])
		v, err := intCmd.Result()
		if err != nil {
			return err
		}

		// set global bucket value to local total limiter
		l.totalLimiter.buckets[index] = v

		// update bucket expire
		_, err = tx.Expire(key, l.totalLimiter.interval).Result()
		//_, err = tx.Pipelined(func(pipe redis.Pipeliner) error {
		//	_, err := pipe.Expire(key, l.totalLimiter.interval).Result()
		//	return err
		//})

		return err
	}, key)

	// sync complete - flush local increment counters
	l.incrementLimiter.buckets[index] = 0

	// construct global limit key
	l.keyPrefix.Truncate(keyPrefixLen)
	l.keyPrefix.WriteString("limit")

	// try to set global limit to default
	if b, err := l.redis.SetNX(l.keyPrefix.String(), l.totalLimiter.limit.value, 0).Result(); err == nil && !b {
		// global limit already exists - overwrite local limit
		if v, err := l.redis.Get(l.keyPrefix.String()).Int64(); err == nil {
			l.totalLimiter.limit.value = v
			l.incrementLimiter.limit.value = v
		}
	}

	l.keyPrefix.Truncate(keyPrefixLen)
}

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

// isAllowed returns TRUE if event is allowed to be processed.
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

// bucketIDToTime converts bucketID to time. This time is start of the bucket.
func (l *inMemoryLimiter) bucketIDToTime(id int) time.Time {
	nano := int64(id) * l.interval.Nanoseconds()
	return time.Unix(nano/100000000, nano%100000000)
}

// timeToBucketID converts time to bucketID.
func (l *inMemoryLimiter) timeToBucketID(t time.Time) int {
	return int(t.UnixNano() / l.interval.Nanoseconds())
}
