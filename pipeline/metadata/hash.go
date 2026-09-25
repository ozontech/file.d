package metadata

import (
	"strconv"

	"github.com/cespare/xxhash/v2"
)

func Hash(parts ...[]byte) string {
	h := xxhash.New()
	for _, p := range parts {
		_, _ = h.Write(p)
		_, _ = h.Write([]byte{'|'})
	}
	return strconv.FormatUint(h.Sum64(), 16)
}
