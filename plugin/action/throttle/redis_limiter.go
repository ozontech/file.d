package throttle

import (
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

const (
	keySuffix = "limit"
)

type redisLimiter struct {
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

// NewRedisLimiter return instance of redis limiter.
func NewRedisLimiter(
	ctx context.Context,
	redis redisClient,
	pipelineName, throttleFieldName, throttleFieldValue string,
	bucketInterval time.Duration,
	bucketCount int,
	limit complexLimit,
) *redisLimiter {
	rl := &redisLimiter{
		redis:            redis,
		incrementLimiter: NewInMemoryLimiter(bucketInterval, bucketCount, limit),
		totalLimiter:     NewInMemoryLimiter(bucketInterval, bucketCount, limit),
	}

	rl.keyPrefix = bytes.Buffer{}

	// full name of keyPrefix will be pipelineName_throttleFieldName_throttleFieldValue_limit. `limit` added afterwards
	rl.keyPrefix.WriteString(pipelineName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")

	return rl
}

func (l *redisLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	// count increment from last sync
	isAllowed := l.incrementLimiter.isAllowed(event, ts)
	if isAllowed {
		// count global limiter if distributed mode on and passed local limit
		isAllowed = l.totalLimiter.isAllowed(event, ts)
	}

	return isAllowed
}

func (l *redisLimiter) sync() {
	// required to prevent concurrent update of buckets via throttle plugin
	l.incrementLimiter.mu.Lock()
	l.totalLimiter.mu.Lock()

	n := time.Now()

	// actualize buckets
	maxID := l.incrementLimiter.rebuildBuckets(n)
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

	l.totalLimiter.mu.Unlock()
	l.incrementLimiter.mu.Unlock()

	l.syncLocalGlobalLimiters(keyIdxs, bucketIDs, maxID)

	l.updateKeyLimit(keyPrefixLen)
}

func (l *redisLimiter) syncLocalGlobalLimiters(keyIdxs, bucketIDs []int, maxID int) {
	prefix := l.keyPrefix.String()

	for i, ID := range bucketIDs {
		i := i
		ID := ID

		bucketIdx := keyIdxs[i]

		stringID := strconv.Itoa(ID)
		key := prefix + stringID

		intCmd := l.redis.IncrBy(key, l.incrementLimiter.buckets[bucketIdx])
		val, err := intCmd.Result()
		if err != nil {
			logger.Errorf("can't watch global limit for %s: %s", key, err.Error())
			continue
		}

		l.updateLimiterValues(maxID, bucketIdx, val)

		// for oldest bucket set lifetime equal to 1 bucket duration, for newest to (bucket count * bucket duration) + 1
		l.redis.Expire(key, l.totalLimiter.interval+l.totalLimiter.interval*time.Duration(bucketIdx))
	}
}

// updateLimiterValues checks probable shift of buckets and updates buckets values.
func (l *redisLimiter) updateLimiterValues(maxID, bucketIdx int, totalLimiterVal int64) {
	l.incrementLimiter.mu.Lock()
	if l.incrementLimiter.maxID == maxID {
		l.incrementLimiter.buckets[bucketIdx] = 0
	} else {
		// buckets were rebuild during request to redis
		shift := l.incrementLimiter.maxID - maxID
		currBucketIdx := bucketIdx - shift

		// currBucketIdx < 0 means it become too old and must be ignored
		if currBucketIdx > 0 {
			l.incrementLimiter.buckets[currBucketIdx] = 0
		}
	}
	l.incrementLimiter.mu.Unlock()

	l.totalLimiter.mu.Lock()
	if l.totalLimiter.maxID == maxID {
		l.totalLimiter.buckets[bucketIdx] = totalLimiterVal
	} else {
		// buckets were rebuild during request to redis
		shift := l.totalLimiter.maxID - maxID
		currBucketIdx := bucketIdx - shift

		// currBucketIdx < 0 means it become too old and must be ignored
		if currBucketIdx > 0 {
			l.totalLimiter.buckets[currBucketIdx] = totalLimiterVal
		}
	}
	l.totalLimiter.mu.Unlock()
}

// updateKeyLimit reads key limit from redis and updates current limit.
func (l *redisLimiter) updateKeyLimit(keyPrefixLen int) {
	l.keyPrefix.WriteString(keySuffix)

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
