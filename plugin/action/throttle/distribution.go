package throttle

import (
	"errors"
	"fmt"
	"math"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/metric"
)

type limitDistributionMetrics struct {
	CustomLabels []string
	EventsCount  metric.HeldCounterVec
	EventsSize   metric.HeldCounterVec
}

type limitDistributionRatio struct {
	Ratio  float64  `json:"ratio"`
	Values []string `json:"values"`
}

type limitDistributionCfg struct {
	Field   string                   `json:"field"`
	Ratios  []limitDistributionRatio `json:"ratios"`
	Enabled bool                     `json:"enabled"`
}

func (c *limitDistributionCfg) isEmpty() bool {
	return c.Field == "" || len(c.Ratios) == 0
}

func parseLimitDistribution(c limitDistributionCfg, totalLimit int64) (limitDistributions, error) {
	if c.Field == "" {
		return limitDistributions{}, nil
	}

	if len(c.Ratios) == 0 {
		return limitDistributions{
			Field:   cfg.ParseFieldSelector(c.Field),
			Enabled: c.Enabled,
		}, nil
	}

	ld := limitDistributions{
		Field:         cfg.ParseFieldSelector(c.Field),
		Distributions: make([]complexDistribution, len(c.Ratios)),
		IdxByKey:      map[string]int{},
		Enabled:       c.Enabled,
	}

	var ratioSum float64
	for i, r := range c.Ratios {
		if r.Ratio < 0 || r.Ratio > 1 {
			return ld, errors.New("'ratio' value must be in range [0.0;1.0]")
		}
		if len(r.Values) == 0 {
			return ld, fmt.Errorf("empty 'values' in ratio #%d", i)
		}

		ratioSum += r.Ratio
		for _, v := range r.Values {
			if _, ok := ld.IdxByKey[v]; ok {
				return ld, fmt.Errorf("value '%s' is duplicated in 'ratios' list", v)
			}
			ld.IdxByKey[v] = i
		}

		ld.Distributions[i] = complexDistribution{
			Ratio: r.Ratio,
			Limit: int64(math.Round(r.Ratio * float64(totalLimit))),
		}
	}

	dif := 1 - ratioSum
	if dif < 0 {
		return ld, errors.New("sum of ratios must be less than or equal to 1")
	}

	defRatio := math.Round(dif*100) / 100
	ld.DefDistribution = complexDistribution{
		Ratio: defRatio,
		Limit: int64(math.Round(defRatio * float64(totalLimit))),
	}

	return ld, nil
}

type complexDistribution struct {
	Ratio float64 `yaml:"ratio"` // between [0.0;1.0]
	Limit int64   `yaml:"limit"` // distributed limit = total limit * ratio
}

// limitDistributions is not thread-safe
type limitDistributions struct {
	Field           []string              `yaml:"field"`    // event field, based on the values of which limits are distributed
	IdxByKey        map[string]int        `yaml:"idxByKey"` // relationship between the field value and index in a distributions
	Distributions   []complexDistribution `yaml:"values"`
	DefDistribution complexDistribution   `yaml:"default"` // default distribution if there is no field value in idxByKey map
	Enabled         bool                  `yaml:"enabled"`
}

func (ld *limitDistributions) isEnabled() bool {
	return ld.Enabled && ld.size() > 0
}

func (ld *limitDistributions) size() int {
	return len(ld.Distributions)
}

// getLimit returns (index, distribution limit) by key or (-1, default distribution limit) otherwise
func (ld *limitDistributions) getLimit(key string) (int, int64) {
	if idx, ok := ld.IdxByKey[key]; ok {
		return idx, ld.Distributions[idx].Limit
	}
	return -1, ld.DefDistribution.Limit
}

func (ld *limitDistributions) copy() limitDistributions {
	fieldCopy := make([]string, len(ld.Field))
	copy(fieldCopy, ld.Field)

	distributionsCopy := make([]complexDistribution, len(ld.Distributions))
	copy(distributionsCopy, ld.Distributions)

	idxByKeyCopy := make(map[string]int, len(ld.IdxByKey))
	for k, v := range ld.IdxByKey {
		idxByKeyCopy[k] = v
	}

	return limitDistributions{
		Field:           fieldCopy,
		Distributions:   distributionsCopy,
		IdxByKey:        idxByKeyCopy,
		DefDistribution: ld.DefDistribution,
		Enabled:         ld.Enabled,
	}
}
