package throttle

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis"
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
			lim := newRedisLimiter(
				&limiterConfig{
					ctx:               ctx,
					redisClient:       tt.args.client,
					pipeline:          pipelineName,
					throttleField:     throttleFieldName,
					bucketInterval:    time.Second,
					bucketsCount:      1,
					limiterValueField: tt.args.valField,
				},
				tt.args.throttleFieldValue,
				tt.args.keyLimitOverride,
				defaultLimit,
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
