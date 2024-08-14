package throttle

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

const (
	keySuffix = "limit"
)

type redisLimiter struct {
	redis redisClient

	// bucket counter prefix, forms key in redis: <keyPrefix>_<bucketID>_<distributionIdx>
	keyPrefix bytes.Buffer
	// limit key in redis. If not overridden, has the format <keyPrefix>_<keySuffix>
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
	// contains bucket values for keys in keyIdxsForSync
	bucketValuesForSync [][]int64
	// contains distribution indexes of distributed bucket for updateLimiterValues
	distributionIdxsForUpdate []int
	// contains distribution values of distributed bucket for updateLimiterValues
	distributionValuesForUpdate []int64

	// json field with limit value
	valField string
	// json field with distribution value
	distributionField string

	// limit default value to set if limit key does not exist in redis
	defaultVal string
}

// newRedisLimiter return instance of redis limiter.
func newRedisLimiter(
	cfg *limiterConfig,
	throttleFieldValue, keyLimitOverride string,
	limit *complexLimit,
	distributionCfg []byte,
	limitDistrMetrics *limitDistributionMetrics,
	nowFn func() time.Time,
) *redisLimiter {
	rl := &redisLimiter{
		redis:             cfg.redisClient,
		incrementLimiter:  newInMemoryLimiter(cfg, limit, limitDistrMetrics, nowFn),
		totalLimiter:      newInMemoryLimiter(cfg, limit, limitDistrMetrics, nowFn),
		valField:          cfg.limiterValueField,
		distributionField: cfg.limiterDistributionField,
	}

	rl.keyIdxsForSync = make([]int, 0, cfg.bucketsCount)
	rl.bucketIdsForSync = make([]int, 0, cfg.bucketsCount)
	rl.bucketValuesForSync = make([][]int64, cfg.bucketsCount)
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
		if limit.distributions.size() > 0 && rl.distributionField != "" {
			distrKey, _ := json.Marshal(rl.distributionField)
			rl.defaultVal = fmt.Sprintf("{%s:%v,%s:%s}",
				valKey, limit.value,
				distrKey, distributionCfg,
			)
		} else {
			rl.defaultVal = fmt.Sprintf("{%s:%v}", valKey, limit.value)
		}
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
		l.bucketValuesForSync[i] = l.bucketValuesForSync[i][:0]

		// no new events passed
		if l.incrementLimiter.isBucketEmpty(i) {
			continue
		}
		l.keyIdxsForSync = append(l.keyIdxsForSync, i)
		l.bucketIdsForSync = append(l.bucketIdsForSync, minID+i)
		l.bucketValuesForSync[i] = l.incrementLimiter.getBucket(i, l.bucketValuesForSync[i])
	}

	l.totalLimiter.unlock()
	l.incrementLimiter.unlock()

	if len(l.bucketIdsForSync) > 0 {
		l.syncLocalGlobalLimiters(maxID)
	}

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

		l.distributionIdxsForUpdate = l.distributionIdxsForUpdate[:0]
		l.distributionValuesForUpdate = l.distributionValuesForUpdate[:0]
		for distrIdx := 0; distrIdx < len(l.bucketValuesForSync[bucketIdx]); distrIdx++ {
			builder.Reset()
			builder.WriteString(key)
			builder.WriteString("_")
			builder.WriteString(strconv.Itoa(distrIdx))

			// <keyPrefix>_<bucketID>_<distributionIdx>
			subKey := builder.String()

			intCmd := l.redis.IncrBy(subKey, l.bucketValuesForSync[bucketIdx][distrIdx])
			val, err := intCmd.Result()
			if err != nil {
				logger.Errorf("can't watch global limit for %s: %s", key, err.Error())
				continue
			}
			l.distributionIdxsForUpdate = append(l.distributionIdxsForUpdate, distrIdx)
			l.distributionValuesForUpdate = append(l.distributionValuesForUpdate, val)

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
			if values == nil {
				lim.resetBucket(actualBucketIdx)
			} else {
				for i, distrIdx := range l.distributionIdxsForUpdate {
					lim.updateBucket(actualBucketIdx, distrIdx, values[i])
				}
			}
		}
		lim.unlock()
	}

	// reset increment limiter
	updateLim(l.incrementLimiter, nil)
	// update total limiter
	updateLim(l.totalLimiter, l.distributionValuesForUpdate)
}

func decodeKeyLimitValue(data []byte, valField, distrField string) (int64, limitDistributionCfg, error) {
	var limit int64
	var distr limitDistributionCfg
	var err error
	var m map[string]json.RawMessage
	reader := bytes.NewReader(data)
	if err = json.NewDecoder(reader).Decode(&m); err != nil {
		return limit, distr, fmt.Errorf("failed to unmarshal map: %w", err)
	}

	limitVal, has := m[valField]
	if !has {
		return limit, distr, fmt.Errorf("no %q key in map", valField)
	}

	if limit, err = json.Number(bytes.Trim(limitVal, `"`)).Int64(); err != nil {
		return limit, distr, err
	}

	if distrField != "" {
		distrVal, has := m[distrField]
		if !has {
			return limit, distr, nil
		}
		if err := json.Unmarshal(distrVal, &distr); err != nil {
			return limit, distr, err
		}
	}

	return limit, distr, nil
}

// updateKeyLimit reads key limit from redis and updates current limit.
func (l *redisLimiter) updateKeyLimit() error {
	var b bool
	var err error
	var limitVal int64
	var distrVal limitDistributionCfg
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
		if limitVal, distrVal, err = decodeKeyLimitValue(jsonData, l.valField, l.distributionField); err != nil {
			return fmt.Errorf("failed to decode redis json value: %w", err)
		}
	} else {
		if limitVal, err = v.Int64(); err != nil {
			return fmt.Errorf("failed to convert redis value to int64: %w", err)
		}
	}
	l.totalLimiter.updateLimit(limitVal)
	l.incrementLimiter.updateLimit(limitVal)

	if err = l.totalLimiter.updateDistribution(distrVal); err != nil {
		return fmt.Errorf("failed to update limiter distribution: %w", err)
	}
	if err = l.incrementLimiter.updateDistribution(distrVal); err != nil {
		return fmt.Errorf("failed to update limiter distribution: %w", err)
	}
	return nil
}

func (l *redisLimiter) setNowFn(fn func() time.Time) {
	l.incrementLimiter.setNowFn(fn)
	l.totalLimiter.setNowFn(fn)
}
