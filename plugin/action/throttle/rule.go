package throttle

import (
	"sort"

	"github.com/ozontech/file.d/pipeline"
)

type complexLimit struct {
	value         int64
	kind          string
	distributions limitDistributions
}

type rule struct {
	fields      []string // sorted list of used keys is used for combining limiter key.
	values      []string // values to check against. order is the same as for keys.
	limit       complexLimit
	byteIdxPart []byte
}

// newRule returns new rule instance.
func newRule(conditions map[string]string, limit complexLimit, ruleNum int) *rule { // nolint: gocritic // hugeParam is ok here
	var (
		keys   = make([]string, 0, len(conditions))
		values = make([]string, len(conditions))
	)

	for k := range conditions {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	for i, k := range keys {
		values[i] = conditions[k]
	}

	byteIdxPart := []byte{byte('a' + ruleNum), ':'}
	return &rule{
		fields:      keys,
		values:      values,
		limit:       limit,
		byteIdxPart: byteIdxPart,
	}
}

// isMatch checks if event has the same field values as given in conditions.
func (r *rule) isMatch(event *pipeline.Event) bool {
	for i, fields := range r.fields {
		if event.Root.Dig(fields).AsString() != r.values[i] {
			return false
		}
	}

	return true
}

// limitDistributions is not thread-safe
type limitDistributions struct {
	field           []string       // event field, based on the values of which limits are distributed
	distributions   []float64      // each distribution is between [0.0;1.0]
	idxByKey        map[string]int // relationship between the field value and index in a distributions
	defDistribution float64        // default distribution if there is no field value in idxByKey map
}

func (lp limitDistributions) size() int {
	return len(lp.distributions)
}

// get returns (index, distribution) by key or (-1, default distribution) otherwise
func (lp limitDistributions) get(key string) (int, float64) {
	if idx, ok := lp.idxByKey[key]; ok {
		return idx, lp.distributions[idx]
	}
	return -1, lp.defDistribution
}

func (lp limitDistributions) copy() limitDistributions {
	fieldCopy := make([]string, len(lp.field))
	copy(fieldCopy, lp.field)

	distributionsCopy := make([]float64, len(lp.distributions))
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
