package throttle

import (
	"encoding/json"
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

func (c *limitDistributionCfg) marshalJson() []byte {
	v, _ := json.Marshal(c)
	return v
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
			field:   cfg.ParseFieldSelector(c.Field),
			enabled: c.Enabled,
		}, nil
	}

	ld := limitDistributions{
		field:         cfg.ParseFieldSelector(c.Field),
		distributions: make([]complexDistribution, len(c.Ratios)),
		idxByKey:      map[string]int{},
		enabled:       c.Enabled,
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
			if _, ok := ld.idxByKey[v]; ok {
				return ld, fmt.Errorf("value '%s' is duplicated in 'ratios' list", v)
			}
			ld.idxByKey[v] = i
		}

		ld.distributions[i] = complexDistribution{
			ratio: r.Ratio,
			limit: int64(math.Round(r.Ratio * float64(totalLimit))),
		}
	}

	dif := 1 - ratioSum
	if dif < 0 {
		return ld, errors.New("sum of ratios must be less than or equal to 1")
	}

	defRatio := math.Round(dif*100) / 100
	ld.defDistribution = complexDistribution{
		ratio: defRatio,
		limit: int64(math.Round(defRatio * float64(totalLimit))),
	}

	return ld, nil
}

type complexDistribution struct {
	ratio float64 // between [0.0;1.0]
	limit int64   // distributed limit = total limit * ratio
}

// limitDistributions is not thread-safe
type limitDistributions struct {
	field           []string       // event field, based on the values of which limits are distributed
	idxByKey        map[string]int // relationship between the field value and index in a distributions
	distributions   []complexDistribution
	defDistribution complexDistribution // default distribution if there is no field value in idxByKey map
	enabled         bool
}

func (ld *limitDistributions) isEnabled() bool {
	return ld.enabled && ld.size() > 0
}

func (ld *limitDistributions) size() int {
	return len(ld.distributions)
}

// getLimit returns (index, distribution limit) by key or (-1, default distribution limit) otherwise
func (ld *limitDistributions) getLimit(key string) (int, int64) {
	if idx, ok := ld.idxByKey[key]; ok {
		return idx, ld.distributions[idx].limit
	}
	return -1, ld.defDistribution.limit
}

func (ld *limitDistributions) copy() limitDistributions {
	fieldCopy := make([]string, len(ld.field))
	copy(fieldCopy, ld.field)

	distributionsCopy := make([]complexDistribution, len(ld.distributions))
	copy(distributionsCopy, ld.distributions)

	idxByKeyCopy := make(map[string]int, len(ld.idxByKey))
	for k, v := range ld.idxByKey {
		idxByKeyCopy[k] = v
	}

	return limitDistributions{
		field:           fieldCopy,
		distributions:   distributionsCopy,
		idxByKey:        idxByKeyCopy,
		defDistribution: ld.defDistribution,
		enabled:         ld.enabled,
	}
}
