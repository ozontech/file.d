package normalize

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewNormalizer(t *testing.T) {
	for _, newFn := range []func() Normalizer{
		NewTokenNormalizer,
	} {
		require.NotPanics(t, func() {
			_ = newFn()
		})
	}
}
