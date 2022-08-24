package throttle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpdateLimiterValuesSameMaxID(t *testing.T) {
	maxID := 10
	bucketCount := 10
	lastBucketIdx := bucketCount - 1
	totalLimitFromRedis := 444444

	incLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	incLimiter.buckets[lastBucketIdx] = 1111111

	totalLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	redisLim := &redisLimiter{
		incrementLimiter: incLimiter,
		totalLimiter:     totalLimiter,
	}

	redisLim.updateLimiterValues(maxID, lastBucketIdx, int64(totalLimitFromRedis))

	assert.Equal(t, maxID, redisLim.incrementLimiter.maxID, "id must be same")
	assert.Equal(t, maxID, redisLim.totalLimiter.maxID, "id must be same")
	assert.Equal(t, int64(0), redisLim.incrementLimiter.buckets[lastBucketIdx], "limit should be discarded")
	assert.Equal(t, int64(totalLimitFromRedis), redisLim.totalLimiter.buckets[lastBucketIdx], "limit didn't updated")
}

func TestUpdateLimiterValuesLastIDShifted(t *testing.T) {
	maxID := 10
	bucketCount := 10
	lastBucketIdx := bucketCount - 1
	totalLimitBeforeSync := 1111
	totalLimitFromRedis := 2222

	incLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	incLimiter.buckets[lastBucketIdx] = 1111111

	totalLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	totalLimiter.buckets[lastBucketIdx] = int64(totalLimitBeforeSync)
	redisLim := &redisLimiter{
		incrementLimiter: incLimiter,
		totalLimiter:     totalLimiter,
	}

	assert.Equal(t, int64(totalLimitBeforeSync), totalLimiter.buckets[lastBucketIdx])
	// maxID was updated
	updateMaxID := 13
	incLimiter.maxID = updateMaxID
	totalLimiter.maxID = updateMaxID

	shift := make([]int64, 3)

	posOfLastBucketAfterShifting := bucketCount - (updateMaxID - maxID) - 1
	incrementLimiterBuckets := incLimiter.buckets[(updateMaxID - maxID):]
	incrementLimiterBuckets = append(incrementLimiterBuckets, shift...)
	redisLim.incrementLimiter.buckets = incrementLimiterBuckets

	totalLimiterBuckets := totalLimiter.buckets[(updateMaxID - maxID):]
	totalLimiterBuckets = append(totalLimiterBuckets, shift...)
	redisLim.totalLimiter.buckets = totalLimiterBuckets

	redisLim.updateLimiterValues(maxID, lastBucketIdx, int64(totalLimitFromRedis))

	assert.Equal(t, int64(0), redisLim.incrementLimiter.buckets[posOfLastBucketAfterShifting], "limit should be discarded")
	assert.Equal(t, int64(totalLimitFromRedis), redisLim.totalLimiter.buckets[posOfLastBucketAfterShifting], "limit didn't updated")
}

func TestUpdateLimiterValuesLastIDOutOfRange(t *testing.T) {
	maxID := 10
	bucketCount := 10
	lastBucketIdx := bucketCount - 1
	totalLimitBeforeSync := 1111
	totalLimitFromRedis := 2222

	incLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	incLimiter.buckets[lastBucketIdx] = 1111111

	totalLimiter := &inMemoryLimiter{
		maxID:       maxID,
		bucketCount: bucketCount,
		buckets:     make([]int64, bucketCount),
	}
	totalLimiter.buckets[lastBucketIdx] = int64(totalLimitBeforeSync)
	redisLim := &redisLimiter{
		incrementLimiter: incLimiter,
		totalLimiter:     totalLimiter,
	}

	assert.Equal(t, int64(totalLimitBeforeSync), totalLimiter.buckets[lastBucketIdx])
	// maxID was shifted out of range
	updateMaxID := 1000
	incLimiter.maxID = updateMaxID
	totalLimiter.maxID = updateMaxID

	redisLim.incrementLimiter.buckets = make([]int64, bucketCount)

	redisLim.totalLimiter.buckets = make([]int64, bucketCount)

	redisLim.updateLimiterValues(maxID, lastBucketIdx, int64(totalLimitFromRedis))

	for i := range redisLim.incrementLimiter.buckets {
		assert.Equal(t, int64(0), redisLim.incrementLimiter.buckets[i], "update vals from redis should be ignored")
		assert.Equal(t, int64(0), redisLim.totalLimiter.buckets[i], "update vals from redis should be ignored")
	}
}
