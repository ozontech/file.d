package throttle

import (
	"bytes"
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
	// bucket counter prefix, forms key in redis: <keyPrefix>_<bucketID>_<shardIdx>
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
	// contains shards indexes of sharded bucket for sync
	shardIdxsForSync []int
	// contains shards values of sharded bucket for sync
	shardValuesForSync []int64

	// json field with limit value
	valField string
	// limit default value to set if limit key does not exist in redis
	defaultVal string
}

// newRedisLimiter return instance of redis limiter.
func newRedisLimiter(
	cfg *limiterConfig,
	throttleFieldValue string,
	keyLimitOverride string,
	limit *complexLimit,
	nowFn func() time.Time,
) *redisLimiter {
	rl := &redisLimiter{
		redis:            cfg.redisClient,
		incrementLimiter: newInMemoryLimiter(cfg, limit, nowFn),
		totalLimiter:     newInMemoryLimiter(cfg, limit, nowFn),
		valField:         cfg.limiterValueField,
	}

	rl.keyIdxsForSync = make([]int, 0, cfg.bucketsCount)
	rl.bucketIdsForSync = make([]int, 0, cfg.bucketsCount)
	rl.keyPrefix = bytes.Buffer{}

	// keyPrefix will be pipelineName_throttleFieldName_throttleFieldValue_
	rl.keyPrefix.WriteString(cfg.pipeline)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(cfg.throttleField)
	rl.keyPrefix.WriteString("_")
	rl.keyPrefix.WriteString(throttleFieldValue)
	rl.keyPrefix.WriteString("_")
	if keyLimitOverride == "" {
		rl.keyLimit = rl.keyPrefix.String() + keySuffix
	} else {
		rl.keyLimit = keyLimitOverride
	}
	if rl.valField == "" {
		rl.defaultVal = strconv.FormatInt(limit.value, 10)
	} else {
		// no err check since valField is string
		valKey, _ := json.Marshal(rl.valField)
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
	l.incrementLimiter.lock()
	l.totalLimiter.lock()

	n := time.Now()

	// actualize buckets
	maxID := l.incrementLimiter.rebuildBuckets(n)
	_ = l.totalLimiter.rebuildBuckets(n)

	minID := l.totalLimiter.bucketsMinID()
	count := l.incrementLimiter.bucketsCount()

	l.keyIdxsForSync = l.keyIdxsForSync[:0]
	l.bucketIdsForSync = l.bucketIdsForSync[:0]
	for i := 0; i < count; i++ {
		// no new events passed
		if l.incrementLimiter.isBucketEmpty(i) {
			continue
		}
		l.keyIdxsForSync = append(l.keyIdxsForSync, i)
		l.bucketIdsForSync = append(l.bucketIdsForSync, minID+i)
	}

	l.totalLimiter.unlock()
	l.incrementLimiter.unlock()

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
	tlBucketsInterval := l.totalLimiter.bucketsInterval()

	for i, ID := range l.bucketIdsForSync {
		builder.Reset()
		builder.WriteString(prefix)
		builder.WriteString(strconv.Itoa(ID))

		// <keyPrefix>_<bucketID>
		key := builder.String()
		bucketIdx := l.keyIdxsForSync[i]

		bucketValues := l.incrementLimiter.getBucket(bucketIdx)
		l.shardIdxsForSync = l.shardIdxsForSync[:0]
		l.shardValuesForSync = l.shardValuesForSync[:0]
		for j := 0; j < len(bucketValues); j++ {
			builder.Reset()
			builder.WriteString(key)
			builder.WriteString("_")
			builder.WriteString(strconv.Itoa(j))

			// <keyPrefix>_<bucketID>_<shardIdx>
			subKey := builder.String()

			intCmd := l.redis.IncrBy(subKey, bucketValues[j])
			val, err := intCmd.Result()
			if err != nil {
				logger.Errorf("can't watch global limit for %s: %s", key, err.Error())
				continue
			}
			l.shardIdxsForSync = append(l.shardIdxsForSync, j)
			l.shardValuesForSync = append(l.shardValuesForSync, val)

			// for oldest bucket set lifetime equal to 1 bucket duration, for newest equal to ((bucket count + 1) * bucket duration)
			l.redis.Expire(subKey, tlBucketsInterval+tlBucketsInterval*time.Duration(bucketIdx))
		}

		l.updateLimiterValues(maxID, bucketIdx)
	}
}

// updateLimiterValues updates buckets values
func (l *redisLimiter) updateLimiterValues(maxID, bucketIdx int) {
	updateLim := func(lim *inMemoryLimiter, values []int64) {
		lim.lock()
		// isn't actual means it becomes too old and must be ignored
		if actualBucketIdx, actual := lim.actualizeBucketIdx(maxID, bucketIdx); actual {
			for i, idx := range l.shardIdxsForSync {
				lim.updateBucket(actualBucketIdx, idx, values[i])
			}
		}
		lim.unlock()
	}

	// reset increment limiter
	updateLim(l.incrementLimiter, make([]int64, len(l.shardValuesForSync)))
	// update total limiter
	updateLim(l.totalLimiter, l.shardValuesForSync)
}

func getLimitValFromJson(data []byte, valField string) (int64, error) {
	var m map[string]json.Number
	reader := bytes.NewReader(data)
	if err := json.NewDecoder(reader).Decode(&m); err != nil {
		return 0, fmt.Errorf("failed to unmarshal map: %w", err)
	}
	limitVal, has := m[valField]
	if !has {
		return 0, fmt.Errorf("no %q key in map", valField)
	}
	return limitVal.Int64()
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
