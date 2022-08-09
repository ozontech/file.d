package throttle

import (
	"sort"

	"github.com/ozontech/file.d/pipeline"
)

type complexLimit struct {
	value int64
	kind  string
}

type rule struct {
	fields      []string // sorted list of used keys is used for combining limiter key.
	values      []string // values to check against. order is the same as for keys.
	limit       complexLimit
	byteIdxPart []byte
}

// NewRule returns new Limit instance.
func NewRule(conditions map[string]string, limit complexLimit, ruleNum int) *rule {
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
