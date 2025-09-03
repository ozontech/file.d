package throttle

import (
	"fmt"
	"slices"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_parseLimitDistribution(t *testing.T) {
	field := "log.level"
	fieldSlice := []string{"log", "level"}

	tests := []struct {
		name       string
		cfg        limitDistributionCfg
		totalLimit int64

		want    limitDistributions
		wantErr bool
	}{
		{
			name: "empty",
			cfg:  limitDistributionCfg{},
			want: limitDistributions{},
		},
		{
			name: "valid_enabled",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"error"}},
					{Ratio: 0.35, Values: []string{"warn", "info"}},
					{Ratio: 0.15, Values: []string{"debug"}},
				},
				Enabled: true,
			},
			totalLimit: 100,
			want: limitDistributions{
				field: fieldSlice,
				distributions: []complexDistribution{
					{ratio: 0.5, limit: 50},
					{ratio: 0.35, limit: 35},
					{ratio: 0.15, limit: 15},
				},
				idxByKey: map[string]int{
					"error": 0,
					"warn":  1, "info": 1,
					"debug": 2,
				},
				enabled: true,
			},
		},
		{
			name: "valid_disabled",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"error"}},
					{Ratio: 0.35, Values: []string{"warn", "info"}},
					{Ratio: 0.15, Values: []string{"debug"}},
				},
				Enabled: false,
			},
			totalLimit: 100,
			want: limitDistributions{
				field: fieldSlice,
				distributions: []complexDistribution{
					{ratio: 0.5, limit: 50},
					{ratio: 0.35, limit: 35},
					{ratio: 0.15, limit: 15},
				},
				idxByKey: map[string]int{
					"error": 0,
					"warn":  1, "info": 1,
					"debug": 2,
				},
				enabled: false,
			},
		},
		{
			name: "valid_with_def",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"error"}},
					{Ratio: 0.3, Values: []string{"warn", "info"}},
					{Ratio: 0.16, Values: []string{"debug"}},
				},
			},
			totalLimit: 100,
			want: limitDistributions{
				field: fieldSlice,
				distributions: []complexDistribution{
					{ratio: 0.5, limit: 50},
					{ratio: 0.3, limit: 30},
					{ratio: 0.16, limit: 16},
				},
				idxByKey: map[string]int{
					"error": 0,
					"warn":  1, "info": 1,
					"debug": 2,
				},
				defDistribution: complexDistribution{
					ratio: 0.04,
					limit: 4,
				},
			},
		},
		{
			name: "valid_empty_ratios",
			cfg: limitDistributionCfg{
				Field:   field,
				Enabled: true,
			},
			totalLimit: 100,
			want: limitDistributions{
				field:   fieldSlice,
				enabled: true,
			},
		},
		{
			name: "err_invalid_ratio1",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: -0.5},
				},
			},
			wantErr: true,
		},
		{
			name: "err_invalid_ratio2",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 1.4},
				},
			},
			wantErr: true,
		},
		{
			name: "err_empty_ratio_values",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.66},
				},
			},
			wantErr: true,
		},
		{
			name: "err_duplicate_ratio_values",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.66, Values: []string{"error"}},
					{Ratio: 0.1, Values: []string{"debug", "error"}},
				},
			},
			wantErr: true,
		},
		{
			name: "err_invalid_ratio_sum",
			cfg: limitDistributionCfg{
				Field: field,
				Ratios: []limitDistributionRatio{
					{Ratio: 0.5, Values: []string{"error"}},
					{Ratio: 0.51, Values: []string{"warn", "info"}},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ld, err := parseLimitDistribution(tt.cfg, tt.totalLimit)

			require.Equal(t, tt.wantErr, err != nil, "wrong error")
			if tt.wantErr {
				return
			}

			require.Equal(t, tt.want.field, ld.field, "wrong field")
			require.Equal(t, tt.want.defDistribution, ld.defDistribution, "wrong defDistribution")
			require.Equal(t, tt.want.enabled, ld.enabled, "wrong enabled")
			require.True(t, slices.Equal(tt.want.distributions, ld.distributions), "wrong distributions size")
			require.Equal(t, len(tt.want.idxByKey), len(ld.idxByKey), "wrong idxByKey size")
			for k, v := range ld.idxByKey {
				require.Equal(t, tt.want.idxByKey[k], v, fmt.Sprintf("wrong value in idxByKey with key %q", k))
			}
		})
	}
}

func TestGetLimitDistributionsCfg(t *testing.T) {
	ld := &limitDistributions{
		field: []string{"log", "level"},
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
	}

	expected := limitDistributionCfg{
		Field: "log.level",
		Ratios: []limitDistributionRatio{
			{Ratio: 0.1, Values: []string{"A", "C"}},
			{Ratio: 0.2, Values: []string{"B"}},
		},
		Enabled: true,
	}

	result := ld.getLimitDistributionsCfg()

	require.Equal(t, expected.Field, result.Field, "wrong Field")
	require.Equal(t, expected.Enabled, result.Enabled, "wrong Enabled")
	require.True(t, isLimitDistributionCfgRatiosEqual(expected.Ratios, result.Ratios), "wrong Ratios")
}

func isLimitDistributionCfgRatiosEqual(expected, result []limitDistributionRatio) bool {
	sort.Slice(result, func(i, j int) bool {
		return result[i].Ratio < result[j].Ratio
	})

	for i, ldRatio := range result {
		slices.Sort(ldRatio.Values)
		if !slices.Equal(expected[i].Values, ldRatio.Values) {
			return false
		}
	}

	return true
}
