package throttle

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
	"github.com/ozontech/file.d/metric"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func Test_updateKeyLimit(t *testing.T) {
	ctx := context.Background()
	pipelineName := "test_pipeline"
	throttleFieldName := "k8s_pod"
	throttleFieldValue1 := "pod1"
	throttleFieldValue2 := "pod2"
	throttleFieldValue3 := "pod3"
	defaultLimit := &complexLimit{
		value: 1,
		kind:  limitKindCount,
	}
	defaultDistribution := limitDistributionCfg{
		Field: "level",
		Ratios: []limitDistributionRatio{
			{
				Ratio:  0.7,
				Values: []string{"error"},
			},
			{
				Ratio:  0.3,
				Values: []string{"warn", "info"},
			},
		},
		Enabled: true,
	}
	defaultDistributionJson := defaultDistribution.marshalJson()
	ld, _ := parseLimitDistribution(defaultDistribution, 10)
	defaultLimitWithDistribution := &complexLimit{
		value:         10,
		kind:          limitKindCount,
		distributions: ld,
	}
	pod1LimitKey := strings.Join([]string{
		pipelineName, throttleFieldName, throttleFieldValue1, keySuffix,
	}, "_")
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
	require.NoError(t, s.Set("custom_limit_key3", `{"custom_limit_field":1000,"custom_distr_field":{"field":"new-field","ratios":[{"ratio":0.4,"values":["val1","val2"]},{"ratio":0.5,"values":["val3"]}],"enabled":false}}`))
	require.NoError(t, s.Set("custom_limit_field_invalid_type", `{"custom_limit_field":{"invalid":"invalid"}}`))
	require.NoError(t, s.Set("custom_distr_field_invalid_type", `{"custom_limit_field":107,"custom_distr_field":"test"}`))
	require.NoError(t, s.Set("custom_limit_field_not_exists", `no_custom_field`))
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
		defaultLimit       *complexLimit
		throttleFieldValue string
		keyLimitOverride   string
		valField           string
		distrField         string
	}
	type limitersData struct {
		limit             int64
		distributions     limitDistributions
		wantSimpleBuckets bool
	}
	type redisData struct {
		key   string
		value string
	}
	tests := []struct {
		name string
		args args

		wantLimiters *limitersData
		wantRedis    *redisData
		wantErr      bool
	}{
		{
			name: "set_default_limit",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "",
				valField:           "",
			},
			wantRedis: &redisData{
				key:   pod1LimitKey,
				value: "1",
			},
		},
		{
			name: "set_default_limit_custom_field",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "default_limit",
				valField:           "custom_limit_field",
			},
			wantRedis: &redisData{
				key:   "default_limit",
				value: `{"custom_limit_field":1}`,
			},
		},
		{
			name: "set_default_distribution",
			args: args{
				client:             client,
				defaultLimit:       defaultLimitWithDistribution,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "default_distr1",
				valField:           "custom_limit_field",
				distrField:         "custom_distr_field",
			},
			wantRedis: &redisData{
				key:   "default_distr1",
				value: fmt.Sprintf(`{"custom_limit_field":10,"custom_distr_field":%s}`, defaultDistributionJson),
			},
		},
		{
			name: "set_default_without_distributions",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "default_distr2",
				valField:           "custom_limit_field",
				distrField:         "custom_distr_field",
			},
			wantRedis: &redisData{
				key:   "default_distr2",
				value: `{"custom_limit_field":1}`,
			},
		},
		{
			name: "set_default_without_distr_field",
			args: args{
				client:             client,
				defaultLimit:       defaultLimitWithDistribution,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "default_distr3",
				valField:           "custom_limit_field",
				distrField:         "",
			},
			wantRedis: &redisData{
				key:   "default_distr3",
				value: `{"custom_limit_field":10}`,
			},
		},
		{
			name: "get_limit_from_default_key",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue2,
				keyLimitOverride:   "",
				valField:           "",
			},
			wantLimiters: &limitersData{
				limit:             101,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_from_custom_key",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key",
				valField:           "",
			},
			wantLimiters: &limitersData{
				limit:             102,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_from_default_key_custom_field",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue3,
				keyLimitOverride:   "",
				valField:           "custom_limit_field",
			},
			wantLimiters: &limitersData{
				limit:             103,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_from_custom_key_custom_field",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key2",
				valField:           "custom_limit_field",
			},
			wantLimiters: &limitersData{
				limit:             104,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_from_custom_key_custom_field_string_value",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_string_val",
				valField:           "custom_limit_field",
			},
			wantLimiters: &limitersData{
				limit:             105,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_and_distribution",
			args: args{
				client:             client,
				defaultLimit:       defaultLimitWithDistribution,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key3",
				valField:           "custom_limit_field",
				distrField:         "custom_distr_field",
			},
			wantLimiters: &limitersData{
				limit: 1000,
				distributions: limitDistributions{
					field: []string{"new-field"},
					idxByKey: map[string]int{
						"val1": 0,
						"val2": 0,
						"val3": 1,
					},
					distributions: []complexDistribution{
						{ratio: 0.4, limit: 400},
						{ratio: 0.5, limit: 500},
					},
					defDistribution: complexDistribution{ratio: 0.1, limit: 100},
					enabled:         false,
				},
				wantSimpleBuckets: false,
			},
		},
		{
			name: "recreate_buckets_simple_to_distributed",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key3",
				valField:           "custom_limit_field",
				distrField:         "custom_distr_field",
			},
			wantLimiters: &limitersData{
				limit: 1000,
				distributions: limitDistributions{
					field: []string{"new-field"},
					idxByKey: map[string]int{
						"val1": 0,
						"val2": 0,
						"val3": 1,
					},
					distributions: []complexDistribution{
						{ratio: 0.4, limit: 400},
						{ratio: 0.5, limit: 500},
					},
					defDistribution: complexDistribution{ratio: 0.1, limit: 100},
					enabled:         false,
				},
				wantSimpleBuckets: false,
			},
		},
		{
			name: "recreate_buckets_distributed_to_simple",
			args: args{
				client:             client,
				defaultLimit:       defaultLimitWithDistribution,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_key2",
				valField:           "custom_limit_field",
				distrField:         "custom_distr_field",
			},
			wantLimiters: &limitersData{
				limit:             104,
				wantSimpleBuckets: true,
			},
		},
		{
			name: "get_limit_from_custom_limit_field_invalid_type",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_field_invalid_type",
				valField:           "custom_limit_field",
			},
			wantErr: true,
		},
		{
			name: "get_distribution_from_custom_distr_field_invalid_type",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_distr_field_invalid_type",
				valField:           "custom_distr_field",
			},
			wantErr: true,
		},
		{
			name: "get_limit_from_custom_limit_field_not_exists",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_limit_field_not_exists",
				valField:           "custom_limit_field",
			},
			wantErr: true,
		},
		{
			name: "parse_int_error",
			args: args{
				client:             client,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "parse_int_error",
			},
			wantErr: true,
		},
		{
			name: "set_limit_error",
			args: args{
				client:             invalidClient,
				defaultLimit:       defaultLimit,
				throttleFieldValue: throttleFieldValue1,
				keyLimitOverride:   "custom_field_error",
				valField:           "custom_limit_field",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctl := metric.NewCtl("test", prometheus.NewRegistry())
			lim := newRedisLimiter(
				&limiterConfig{
					ctx:                      ctx,
					redisClient:              tt.args.client,
					pipeline:                 pipelineName,
					throttleField:            throttleFieldName,
					bucketInterval:           time.Second,
					bucketsCount:             1,
					limiterValueField:        tt.args.valField,
					limiterDistributionField: tt.args.distrField,
				},
				tt.args.throttleFieldValue,
				tt.args.keyLimitOverride,
				tt.args.defaultLimit,
				defaultDistributionJson,
				&limitDistributionMetrics{
					EventsCount: metric.NewHeldCounterVec(ctl.RegisterCounterVec("test_count", "")),
					EventsSize:  metric.NewHeldCounterVec(ctl.RegisterCounterVec("test_size", "")),
				},
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

			if tt.wantLimiters != nil {
				require.Equal(t, tt.wantLimiters.limit, lim.incrementLimiter.limit.value)
				require.Equal(t, tt.wantLimiters.limit, lim.totalLimiter.limit.value)

				require.Equal(t, tt.wantLimiters.distributions, lim.incrementLimiter.limit.distributions)
				require.Equal(t, tt.wantLimiters.distributions, lim.totalLimiter.limit.distributions)

				require.Equal(t, tt.wantLimiters.wantSimpleBuckets, lim.incrementLimiter.buckets.isSimple())
				require.Equal(t, tt.wantLimiters.wantSimpleBuckets, lim.totalLimiter.buckets.isSimple())
			}
			if tt.wantRedis != nil {
				val, err := s.Get(tt.wantRedis.key)
				require.NoError(t, err)
				require.Equal(t, tt.wantRedis.value, val)
			}
		})
	}
	t.Cleanup(func() {
		s.Close()
	})
}

func Test_decodeKeyLimitValue(t *testing.T) {
	type args struct {
		data       []byte
		valField   string
		distrField string
	}
	tests := []struct {
		name string
		args args

		wantLimit int64
		wantDistr limitDistributionCfg
		wantErr   bool
	}{
		{
			name: "ok_only_limit",
			args: args{
				data:     []byte(`{"limit_key":"3000"}`),
				valField: "limit_key",
			},
			wantLimit: 3000,
		},
		{
			name: "ok_with_object",
			args: args{
				data:     []byte(`{"limit_key":"3000","some_obj":{"field":"key"}}`),
				valField: "limit_key",
			},
			wantLimit: 3000,
		},
		{
			name: "ok_limit_and_distribution",
			args: args{
				data:       []byte(`{"limit_key":"3000","distr_key":{"field":"my-field","ratios":[{"ratio":0.4,"values":["val1","val2"]},{"ratio":0.6,"values":["val3"]}],"enabled":true}}`),
				valField:   "limit_key",
				distrField: "distr_key",
			},
			wantLimit: 3000,
			wantDistr: limitDistributionCfg{
				Field: "my-field",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.4, Values: []string{"val1", "val2"}},
					{Ratio: 0.6, Values: []string{"val3"}},
				},
				Enabled: true,
			},
		},
		{
			name: "decode_error",
			args: args{
				data:     []byte(`"3000"`),
				valField: "limit_key",
			},
			wantErr: true,
		},
		{
			name: "limit_key_not_exists",
			args: args{
				data:     []byte(`{"not_limit_key":"3000"}`),
				valField: "limit_key",
			},
			wantErr: true,
		},
		{
			name: "limit_format_error",
			args: args{
				data:     []byte(`{"limit_key":"not_int"}`),
				valField: "limit_key",
			},
			wantErr: true,
		},
		{
			name: "distribution_key_not_exists",
			args: args{
				data:       []byte(`{"limit_key":"3000","not_distr_key":"test"}`),
				valField:   "limit_key",
				distrField: "distr_key",
			},
			wantLimit: 3000,
		},
		{
			name: "distribution_format_error",
			args: args{
				data:       []byte(`{"limit_key":"3000","distr_key":"test"}`),
				valField:   "limit_key",
				distrField: "distr_key",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotLimit, gotDistr, err := decodeKeyLimitValue(tt.args.data, tt.args.valField, tt.args.distrField)

			require.Equal(t, tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}

			require.Equal(t, tt.wantLimit, gotLimit)
			require.Equal(t, tt.wantDistr, gotDistr)
		})
	}
}
