package throttle

import (
	"bytes"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/ozontech/file.d/pipeline"
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

	// Full name of metric will be $throttleFieldName_$throttleFieldValue_limit.
	// `limit` added afterwards.
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
