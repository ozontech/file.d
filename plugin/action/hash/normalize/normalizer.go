package normalize

import (
	"fmt"
)

type Normalizer interface {
	Normalize(out, data []byte) []byte
}

func formatPlaceholder(v string) []byte {
	return []byte(fmt.Sprintf("<%s>", v))
}
