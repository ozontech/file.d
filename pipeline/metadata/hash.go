package metadata

import (
	"github.com/cespare/xxhash/v2"
)

func Hash(parts ...string) uint64 {
	h := xxhash.New()
	for _, p := range parts {
		_, _ = h.WriteString(p)
		_, _ = h.WriteString("|")
	}
	return h.Sum64()
}
