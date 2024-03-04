package throttle

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
}

func (lp limitDistributions) size() int {
	return len(lp.distributions)
}

// get returns (index, distribution limit) by key or (-1, default distribution limit) otherwise
func (lp limitDistributions) getLimit(key string) (int, int64) {
	if idx, ok := lp.idxByKey[key]; ok {
		return idx, lp.distributions[idx].limit
	}
	return -1, lp.defDistribution.limit
}

func (lp limitDistributions) copy() limitDistributions {
	fieldCopy := make([]string, len(lp.field))
	copy(fieldCopy, lp.field)

	distributionsCopy := make([]complexDistribution, len(lp.distributions))
	copy(distributionsCopy, lp.distributions)

	idxByKeyCopy := make(map[string]int, len(lp.idxByKey))
	for k, v := range lp.idxByKey {
		idxByKeyCopy[k] = v
	}

	return limitDistributions{
		field:           fieldCopy,
		distributions:   distributionsCopy,
		idxByKey:        idxByKeyCopy,
		defDistribution: lp.defDistribution,
	}
}
