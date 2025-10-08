package throttle

import (
	"testing"

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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.limiter.isLimitCfgChanged(tt.curLimit, tt.curDistribution)
			require.Equal(t, tt.want, got)
		})
	}
}
