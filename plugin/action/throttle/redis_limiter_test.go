package throttle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_updateKeyLimit(t *testing.T) {
	ctx := context.Background()
	pipelineName := "test_pipeline"
	throttleFieldName := "k8s_pod"
	throttleFieldValue1 := "pod1"
	throttleFieldValue2 := "pod2"
	throttleFieldValue3 := "pod3"
	defaultLimit := complexLimit{
		value: 1,
		kind:  "count",
	}
	pod2LimitKey := strings.Join([]string{
		pipelineName, throttleFieldName, throttleFieldValue2, keySuffix,
	}, "_")
	pod3LimitKey := strings.Join([]string{
		pipelineName, throttleFieldName, throttleFieldValue3, keySuffix,
	}, "_")

	s, err := miniredis.Run()
	require.NoError(t, err)

	require.NoError(t, s.Set(pod2LimitKey, "101"))
	require.NoError(t, s.Set("custom_limit_key", "102"))
	require.NoError(t, s.Set(pod3LimitKey, `{"custom_limit_field":103}`))
	require.NoError(t, s.Set("custom_limit_key2", `{"custom_limit_field":104}`))
	require.NoError(t, s.Set("custom_field_string_val", `{"custom_limit_field":"105"}`))
	require.NoError(t, s.Set("custom_field_invalid_type", `{"custom_limit_field":{"invalid":"invalid"}}`))
	require.NoError(t, s.Set("custom_field_error", `no_custom_field`))
	require.NoError(t, s.Set("parse_int_error", `not_int`))

	client := redis.NewClient(
		&redis.Options{
			Network:         "tcp",
			Addr:            s.Addr(),
			Password:        "",
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			MaxRetries:      0,
			MinRetryBackoff: 0,
			MaxRetryBackoff: 0,
		},
	)

	invalidClient := redis.NewClient(
		&redis.Options{
			Network:         "tcp",
			Addr:            "invalid",
			Password:        "",
			ReadTimeout:     time.Second,
			WriteTimeout:    time.Second,
			MaxRetries:      0,
			MinRetryBackoff: 0,
			MaxRetryBackoff: 0,
		},
	)

	type args struct {
		client             redisClient
		throttleFieldValue string
		keyLimitOverride   string
		valField           string
	}
	tests := []struct {
		name      string
		args      args
		wantLimit int64
		wantErr   bool
	}{
		{
			name: "set_default_limit",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "",
				valField:           "",
			},
			wantLimit: 1,
			wantErr:   false,
		},
		{
			name: "get_limit_from_default_key",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue2,
				keyLimitOverride:   "",
				valField:           "",
			},
			wantLimit: 101,
			wantErr:   false,
		},
		{
			name: "get_limit_from_custom_key",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key",
				valField:           "",
			},
			wantLimit: 102,
			wantErr:   false,
		},
		{
			name: "get_limit_from_default_key_custom_field",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue3,
				keyLimitOverride:   "",
				valField:           "custom_limit_field",
			},
			wantLimit: 103,
			wantErr:   false,
		},
		{
			name: "get_limit_from_custom_key_custom_field",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key2",
				valField:           "custom_limit_field",
			},
			wantLimit: 104,
			wantErr:   false,
		},
		{
			name: "get_limit_from_custom_key_custom_field_string_value",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_string_val",
				valField:           "custom_limit_field",
			},
			wantLimit: 105,
			wantErr:   false,
		},
		{
			name: "get_limit_from_custom_field_invalid_val_type",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_invalid_type",
				valField:           "custom_limit_field",
			},
			wantLimit: 0,
			wantErr:   true,
		},
		{
			name: "get_limit_from_custom_field_error",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_error",
				valField:           "custom_limit_field",
			},
			wantLimit: 0,
			wantErr:   true,
		},
		{
			name: "parse_int_error",
			args: args{
				client:             client,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "parse_int_error",
			},
			wantLimit: 0,
			wantErr:   true,
		},
		{
			name: "set_limit_error",
			args: args{
				client:             invalidClient,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_error",
				valField:           "custom_limit_field",
			},
			wantLimit: 0,
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			lim := NewRedisLimiter(
				ctx,
				tt.args.client,
				pipelineName,
				throttleFieldName,
				tt.args.throttleFieldValue,
				time.Second,
				1,
				defaultLimit,
				tt.args.keyLimitOverride,
				tt.args.valField,
				time.Now,
			)
			err := lim.updateKeyLimit()
			errMsg := ""
			if err != nil {
				errMsg = err.Error()
			}
			require.Equal(t, tt.wantErr, err != nil, errMsg)
			if tt.wantErr {
				return
			}
			require.Equal(t, tt.wantLimit, lim.incrementLimiter.limit.value)
			require.Equal(t, tt.wantLimit, lim.totalLimiter.limit.value)
		})
	}
	t.Cleanup(func() {
		s.Close()
	})
}

func Test_getLimitValFromJson(t *testing.T) {
	type args struct {
		data     []byte
		valField string
	}
	tests := []struct {
		name    string
		args    args
		want    int64
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				data:     []byte(`{"limit_key":"3000"}`),
				valField: "limit_key",
			},
			want:    3000,
			wantErr: false,
		},
		{
			name: "unmarshal_error",
			args: args{
				data:     []byte(`"3000"`),
				valField: "limit_key",
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "key_error",
			args: args{
				data:     []byte(`{"not_limit_key":"3000"}`),
				valField: "limit_key",
			},
			want:    0,
			wantErr: true,
		},
		{
			name: "parse_int64_error",
			args: args{
				data:     []byte(`{"limit_key":"not_int"}`),
				valField: "limit_key",
			},
			want:    0,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := getLimitValFromJson(tt.args.data, tt.args.valField)
			require.Equal(t, tt.wantErr, err != nil)
			require.Equal(t, tt.want, got)
		})
	}
}
