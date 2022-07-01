package throttle

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

// interface with only necessary functions of the original redis.Client
type redisClient interface {
	Watch(func(tx *redis.Tx) error, ...string) error
	Expire(key string, expiration time.Duration) *redis.BoolCmd
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Get(key string) *redis.StringCmd
	Ping() *redis.StatusCmd
}

const (
	keyPostfix = "limit"
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

	// isDistributed regulates using of totalLimiter
	isDistributed bool
}

func NewRedisLimiter(
	redis redisClient,
	throttleFieldName, throttleFieldValue string,
	refreshInterval, bucketInterval time.Duration,
	bucketCount int,
	limit complexLimit,
	isDistributed bool,
) *redisLimiter {
	var totalLimiter *inMemoryLimiter
	if isDistributed {
		totalLimiter = NewInMemoryLimiter(bucketInterval, bucketCount, limit)
	}
	rl := &redisLimiter{
		redis:            redis,
		incrementLimiter: NewInMemoryLimiter(bucketInterval, bucketCount, limit),
		totalLimiter:     totalLimiter,
		isDistributed:    isDistributed,
	}

	rl.keyPrefix = bytes.Buffer{}

	// full name of metric will be $throttleFieldName_$throttleFieldValue_limit `limit` added afterwards
	rl.keyPrefix.WriteString(throttleFieldName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")

	fmt.Println(rl.keyPrefix.String())
	ticker := time.NewTicker(refreshInterval)
	go func() {
		for range ticker.C {
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
	if ret && l.isDistributed {
		// count global limiter if distributed mode on and passed local limit
		ret = l.totalLimiter.isAllowed(event, ts)
	}

	return ret
}

func (l *redisLimiter) syncRedis() {
	l.mu.Lock()
	defer l.mu.Unlock()

	n := time.Now()
	l.incrementLimiter.rebuildBuckets(n)

	// if not distriburted just sync limit with redis
	if !l.isDistributed {
		l.tryResetlLocalLimit()
		return
	}

	l.totalLimiter.rebuildBuckets(n)

	id := l.totalLimiter.timeToBucketID(n)
	index := id - l.totalLimiter.minID

	// get actual bucket value
	keyPrefixLen := l.keyPrefix.Len()
	l.keyPrefix.WriteString(strconv.Itoa(id))

	if err := l.watchGlobalLimit(index); err != nil {
		logger.Errorf("can't watch global limit: %s", err.Error())
	}

	// sync complete - flush local increment counters
	l.incrementLimiter.buckets[index] = 0

	l.tryResetlLimits(keyPrefixLen)
}

func (l *redisLimiter) tryResetlLocalLimit() {
	// get actual bucket value
	keyPrefixLen := l.keyPrefix.Len()
	// construct global limit key
	l.keyPrefix.Truncate(keyPrefixLen)
	l.keyPrefix.WriteString(keyPostfix)
	defer func() { l.keyPrefix.Truncate(keyPrefixLen) }()

	if b, err := l.redis.SetNX(l.keyPrefix.String(), l.incrementLimiter.limit.value, 0).Result(); err == nil && !b {
		if v, err := l.redis.Get(l.keyPrefix.String()).Int64(); err == nil {
			l.incrementLimiter.limit.value = v
		}
	}
}

func (l *redisLimiter) tryResetlLimits(keyPrefixLen int) {
	l.keyPrefix.Truncate(keyPrefixLen)
	l.keyPrefix.WriteString(keyPostfix)
	defer func() { l.keyPrefix.Truncate(keyPrefixLen) }()

	// try to set global limit to default
	if b, err := l.redis.SetNX(l.keyPrefix.String(), l.totalLimiter.limit.value, 0).Result(); err == nil && !b {
		// global limit already exists - overwrite local limit
		if v, err := l.redis.Get(l.keyPrefix.String()).Int64(); err == nil {
			l.totalLimiter.limit.value = v
			l.incrementLimiter.limit.value = v
		}
	}
}

func (l *redisLimiter) watchGlobalLimit(index int) error {
	key := l.keyPrefix.String()

	return l.redis.Watch(func(tx *redis.Tx) error {
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

		return err
	}, key)
}
