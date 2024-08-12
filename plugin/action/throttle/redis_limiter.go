package throttle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
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
	// bucket counter prefix bucket id forms key in redis: <keyPrefix>_<bucketID>
	keyPrefix bytes.Buffer

	// <keyPrefix>_<keySuffix>
	// limit key in redis
	keyLimit string

	// contains values which will be used for incrementing remote bucket counter
	// buckets will be flushed after every sync to contain only increment value
	incrementLimiter *inMemoryLimiter

	// contains global values synced from redis
	totalLimiter *inMemoryLimiter

	// contains indexes of buckets in incrementLimiter for sync
	keyIdxsForSync []int

	// contains bucket IDs for keys in keyIdxsForSync
	bucketIdsForSync []int

	// json field with limit value
	valField string

	// limit default value to set if limit key does not exist in redis
	defaultVal string
}

// NewRedisLimiter return instance of redis limiter.
func NewRedisLimiter(
	ctx context.Context,
	redis redisClient,
	pipelineName, throttleFieldName, throttleFieldValue string,
	bucketInterval time.Duration,
	bucketCount int,
	limit complexLimit,
	keyLimitOverride string,
	valField string,
	nowFn func() time.Time,
) *redisLimiter {
	rl := &redisLimiter{
		redis:            redis,
		incrementLimiter: NewInMemoryLimiter(bucketInterval, bucketCount, limit, nowFn),
		totalLimiter:     NewInMemoryLimiter(bucketInterval, bucketCount, limit, nowFn),
		valField:         valField,
	}

	rl.keyIdxsForSync = make([]int, 0, bucketCount)
	rl.bucketIdsForSync = make([]int, 0, bucketCount)
	rl.keyPrefix = bytes.Buffer{}

	// full name of keyPrefix will be pipelineName_throttleFieldName_throttleFieldValue_limit. `limit` added afterwards
	rl.keyPrefix.WriteString(pipelineName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldName)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")
	if keyLimitOverride == "" {
		rl.keyLimit = rl.keyPrefix.String() + keySuffix
	} else {
		rl.keyLimit = keyLimitOverride
	}
	if valField == "" {
		rl.defaultVal = strconv.FormatInt(limit.value, 10)
	} else {
		// no err check since valField is string
		valKey, _ := json.Marshal(valField)
		rl.defaultVal = fmt.Sprintf("{%s:%v}", valKey, limit.value)
	}

	return rl
}

func (l *redisLimiter) isAllowed(event *pipeline.Event, ts time.Time) bool {
	// count increment from last sync
	isAllowed := l.incrementLimiter.isAllowed(event, ts)
	if isAllowed {
		// count global limiter from last sync
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

	l.keyIdxsForSync = l.keyIdxsForSync[:0]
	l.bucketIdsForSync = l.bucketIdsForSync[:0]
	for i := 0; i < count; i++ {
		// no new events passed
		if l.incrementLimiter.buckets[i] == 0 {
			continue
		}
		l.keyIdxsForSync = append(l.keyIdxsForSync, i)
		l.bucketIdsForSync = append(l.bucketIdsForSync, minID+i)
	}

	l.totalLimiter.mu.Unlock()
	l.incrementLimiter.mu.Unlock()

	if len(l.bucketIdsForSync) == 0 {
		return
	}

	l.syncLocalGlobalLimiters(maxID)
	if err := l.updateKeyLimit(); err != nil {
		logger.Errorf("failed to update key limit: %v", err)
	}
}

func (l *redisLimiter) syncLocalGlobalLimiters(maxID int) {
	prefix := l.keyPrefix.String()

	builder := new(strings.Builder)

	for i, ID := range l.bucketIdsForSync {
		builder.Reset()
		builder.WriteString(prefix)

		bucketIdx := l.keyIdxsForSync[i]

		stringID := strconv.Itoa(ID)
		builder.WriteString(stringID)
		key := builder.String()

		intCmd := l.redis.IncrBy(key, l.incrementLimiter.buckets[bucketIdx])
		val, err := intCmd.Result()
		if err != nil {
			logger.Errorf("can't watch global limit for %s: %s", key, err.Error())
			continue
		}

		l.updateLimiterValues(maxID, bucketIdx, val)

		// for oldest bucket set lifetime equal to 1 bucket duration, for newest equal to ((bucket count + 1) * bucket duration)
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

func getLimitValFromJson(data []byte, valField string) (int64, error) {
	var m map[string]json.RawMessage
	reader := bytes.NewReader(data)
	if err := json.NewDecoder(reader).Decode(&m); err != nil {
		return 0, fmt.Errorf("failed to unmarshal map: %w", err)
	}
	limitVal, has := m[valField]
	if !has {
		return 0, fmt.Errorf("no %q key in map", valField)
	}
	return json.Number(bytes.Trim(limitVal, `"`)).Int64()
}

// updateKeyLimit reads key limit from redis and updates current limit.
func (l *redisLimiter) updateKeyLimit() error {
	var b bool
	var err error
	var limitVal int64
	// try to set global limit to default
	if b, err = l.redis.SetNX(l.keyLimit, l.defaultVal, 0).Result(); err != nil {
		return fmt.Errorf("failed to set redis value by key %q: %w", l.keyLimit, err)
	} else if b {
		return nil
	}
	// global limit already exists - overwrite local limit
	v := l.redis.Get(l.keyLimit)
	if l.valField != "" {
		var jsonData []byte
		if jsonData, err = v.Bytes(); err != nil {
			return fmt.Errorf("failed to convert redis value to bytes: %w", err)
		}
		if limitVal, err = getLimitValFromJson(jsonData, l.valField); err != nil {
			return fmt.Errorf("failed to get limit value from redis json: %w", err)
		}
	} else {
		if limitVal, err = v.Int64(); err != nil {
			return fmt.Errorf("failed to convert redis value to int64: %w", err)
		}
	}
	// atomic store to prevent races with limit value fast check
	atomic.StoreInt64(&l.totalLimiter.limit.value, limitVal)
	atomic.StoreInt64(&l.incrementLimiter.limit.value, limitVal)
	return nil
}

func (l *redisLimiter) setNowFn(fn func() time.Time) {
	l.incrementLimiter.setNowFn(fn)
	l.totalLimiter.setNowFn(fn)
}
