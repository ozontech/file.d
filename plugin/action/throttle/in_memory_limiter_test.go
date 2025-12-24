package throttle

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIsLimitCfgChangedSame(t *testing.T) {
	tests := []struct {
		name            string
		limiter         *inMemoryLimiter
		curLimit        int64
		curDistribution []limitDistributionRatio

		want bool
	}{
		{
			name: "same_limit_and_distribution",
			limiter: &inMemoryLimiter{
				limit: complexLimit{
					value: 100,
					distributions: limitDistributions{
						idxByKey: map[string]int{
							"A": 0,
							"B": 1,
						},
						distributions: []complexDistribution{
							{ratio: 0.1, limit: 10},
							{ratio: 0.2, limit: 20},
						},
						enabled: true,
					},
				},
			},
			curLimit: 100,
			curDistribution: []limitDistributionRatio{
				{Ratio: 0.1, Values: []string{"A"}},
				{Ratio: 0.2, Values: []string{"B"}},
			},

			want: false,
		},
		{
			name: "limit_changed",
			limiter: &inMemoryLimiter{
				limit: complexLimit{
					value: 200,
				},
			},
			curLimit:        100,
			curDistribution: []limitDistributionRatio{},

			want: true,
		},
		{
			name: "distribution_changed",
			limiter: &inMemoryLimiter{
				limit: complexLimit{
					value: 100,
					distributions: limitDistributions{
						idxByKey: map[string]int{
							"A": 0,
							"B": 0,
						},
						distributions: []complexDistribution{
							{ratio: 0.1, limit: 10},
						},
						enabled: true,
					},
				},
			},
			curLimit: 100,
			curDistribution: []limitDistributionRatio{
				{Ratio: 0.1, Values: []string{"A"}},
				{Ratio: 0.2, Values: []string{"B"}},
			},

			want: true,
		},
		{
			name: "increase_distributions_size",
			limiter: &inMemoryLimiter{
				limit: complexLimit{
					value: 100,
					distributions: limitDistributions{
						idxByKey: map[string]int{
							"A": 0,
							"B": 1,
							"C": 0,
						},
						distributions: []complexDistribution{
							{ratio: 0.1, limit: 10},
							{ratio: 0.2, limit: 20},
						},
						enabled: true,
					},
				},
			},
			curLimit: 100,
			curDistribution: []limitDistributionRatio{
				{Ratio: 0.1, Values: []string{"A"}},
				{Ratio: 0.2, Values: []string{"B"}},
			},

			want: true,
		},
		{
			name: "decrease_distributions_size",
			limiter: &inMemoryLimiter{
				limit: complexLimit{
					value: 100,
					distributions: limitDistributions{
						idxByKey: map[string]int{
							"A": 0,
						},
						distributions: []complexDistribution{
							{ratio: 0.1, limit: 10},
						},
						enabled: true,
					},
				},
			},
			curLimit: 100,
			curDistribution: []limitDistributionRatio{
				{Ratio: 0.1, Values: []string{"A"}},
				{Ratio: 0.2, Values: []string{"B"}},
			},

			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.limiter.isLimitCfgChanged(tt.curLimit, tt.curDistribution)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestInMemoryLimiterUpdateDistribution(t *testing.T) {
	const totalLimit int64 = 100

	tests := []struct {
		name string
		cur  limitDistributionCfg
		new  limitDistributionCfg

		wantDistrField        []string
		wantBucketsDistrCount int
		wantErr               bool
	}{
		{
			name:                  "both_empty",
			cur:                   limitDistributionCfg{},
			new:                   limitDistributionCfg{},
			wantDistrField:        nil,
			wantBucketsDistrCount: 0,
		},
		{
			name: "cur_empty",
			cur:  limitDistributionCfg{},
			new: limitDistributionCfg{
				Field: "test1",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"val1", "val2"}},
				},
			},
			wantDistrField:        []string{"test1"},
			wantBucketsDistrCount: 2,
		},
		{
			name: "new_empty",
			cur: limitDistributionCfg{
				Field: "test1",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"val1", "val2"}},
				},
			},
			new:                   limitDistributionCfg{},
			wantDistrField:        nil,
			wantBucketsDistrCount: 0,
		},
		{
			name: "err_parsing_new",
			cur: limitDistributionCfg{
				Field: "test1",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"val1", "val2"}},
				},
			},
			new: limitDistributionCfg{
				Field: "test2",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"val3", "val4"}},
					{Ratio: 10, Values: []string{"val5", "val6"}},
				},
			},
			wantDistrField:        []string{"test1"},
			wantBucketsDistrCount: 2,
			wantErr:               true,
		},
		{
			name: "both_not_empty",
			cur: limitDistributionCfg{
				Field: "test1",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"val1", "val2"}},
				},
			},
			new: limitDistributionCfg{
				Field: "test2",
				Ratios: []limitDistributionRatio{
					{Ratio: 0.3, Values: []string{"val3", "val4"}},
					{Ratio: 0.4, Values: []string{"val5"}},
				},
			},
			wantDistrField:        []string{"test2"},
			wantBucketsDistrCount: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l, err := initTestInmemLimiter(tt.cur, totalLimit)
			require.NoError(t, err)

			err = l.updateDistribution(tt.new)
			require.Equal(t, tt.wantErr, err != nil)
			require.Equal(t, tt.wantDistrField, l.limit.distributions.field)
			require.Equal(t, tt.wantBucketsDistrCount, l.buckets.getDistrCount())
		})
	}
}

func initTestInmemLimiter(distrCfg limitDistributionCfg, totalLimit int64) (*inMemoryLimiter, error) {
	distr, err := parseLimitDistribution(distrCfg, totalLimit)
	if err != nil {
		return nil, err
	}

	return &inMemoryLimiter{
		limit: complexLimit{
			value:         totalLimit,
			kind:          limitKindCount,
			distributions: distr,
		},
		buckets: newBuckets(
			1,
			distr.size()+1, // +1 because of default distribution
			time.Second,
		),
	}, nil
}
