package throttle

import (
	"bytes"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

const (
	keySuffix = "limit"
)

type redisLimiter struct {
	mu    sync.Mutex
	redis redisClient

	// <keyPrefix>_
	// will be used for bucket counter <keyPrefix>_<bucketID> and limit key <keyPrefix>_limit
	keyPrefix bytes.Buffer

	// contains values which will be used for incrementing remote bucket counter
	// buckets will be flushed after every sync to contain only increment value
	incrementLimiter *inMemoryLimiter

	// contains global values synced from redis
	totalLimiter *inMemoryLimiter
}

func NewRedisLimiter(
	ctx context.Context,
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

	// full name of keyPrefix will be $throttleFieldName_$throttleFieldValue_limit. `limit` added afterwards
	rl.keyPrefix.WriteString(throttleFieldName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")

	ticker := time.NewTicker(refreshInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.sync()
			}
		}
	}()

	return rl
}

func (l *redisLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	// count increment from last sync
	isAllowed := l.incrementLimiter.isAllowed(event, ts)
	if isAllowed {
		// count global limiter if distributed mode on and passed local limit
		isAllowed = l.totalLimiter.isAllowed(event, ts)
	}

	return isAllowed
}

func (l *redisLimiter) sync() {
	l.mu.Lock()
	defer l.mu.Unlock()

	// required to prevent current update of buckets via throttle plugin
	l.incrementLimiter.mu.Lock()
	l.totalLimiter.mu.Lock()

	n := time.Now()

	// actualize buckets
	_ = l.incrementLimiter.rebuildBuckets(n)
	_ = l.totalLimiter.rebuildBuckets(n)

	minID := l.totalLimiter.minID
	count := l.incrementLimiter.bucketCount

	// get actual bucket value
	keyPrefixLen := l.keyPrefix.Len()

	keyIdxs := make([]int, 0, count)
	bucketIDs := make([]int, 0, count)

	for i := 0; i < count; i++ {
		// no new events passed
		if l.incrementLimiter.buckets[i] == 0 {
			continue
		}
		keyIdxs = append(keyIdxs, i)
		bucketIDs = append(bucketIDs, minID+i)
	}

	l.syncLocalGlobalLimiters(keyPrefixLen, keyIdxs, bucketIDs)

	l.totalLimiter.mu.Unlock()
	l.incrementLimiter.mu.Unlock()

	l.updateKeyLimit(keyPrefixLen)
}

func (l *redisLimiter) syncLocalGlobalLimiters(keyPrefixLen int, keyIdxs, bucketIDs []int) {
	prefix := l.keyPrefix.String()
	var wg sync.WaitGroup

	wg.Add(len(bucketIDs))
	for i, ID := range bucketIDs {
		i := i
		ID := ID

		go func() {
			defer wg.Done()

			stringID := strconv.Itoa(ID)
			key := prefix + stringID

			intCmd := l.redis.IncrBy(key, l.incrementLimiter.buckets[keyIdxs[i]])
			val, err := intCmd.Result()
			if err != nil {
				logger.Errorf("can't watch global limit for %s: %s", key, err.Error())
				return
			}

			l.incrementLimiter.buckets[keyIdxs[i]] = 0
			l.totalLimiter.buckets[keyIdxs[i]] = val

			// for oldest bucket set lifetime equal to 1 bucket duration, for newest to (bucket count * bucket duration) + 1
			l.redis.Expire(key, l.totalLimiter.interval+l.totalLimiter.interval*time.Duration(keyIdxs[i]))
		}()
	}
	wg.Wait()
}

// updateKeyLimit reads key limit from redis and updates current limit.
func (l *redisLimiter) updateKeyLimit(keyPrefixLen int) {
	l.keyPrefix.Truncate(keyPrefixLen)
	l.keyPrefix.WriteString(keySuffix)
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
