package file

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPairUpdate(t *testing.T) {
	pair := NewPair()

	min := int64(1)
	max := int64(949289429149812319)
	candidates := []int64{
		max,
		rand.Int63n(max),
		rand.Int63n(max),
		rand.Int63n(max),
		min,
		rand.Int63n(max),
		rand.Int63n(max),
		rand.Int63n(max),
	}

	pair.UpdatePair(candidates...)

	require.Equal(t, min, pair.min)
	require.Equal(t, max, pair.max)
}

func TestPairGet(t *testing.T) {
	pair := NewPair()

	min := int64(33)
	max := int64(6666)
	pair.min = min
	pair.max = max

	getMin, getMax := pair.Get()

	require.Equal(t, min, getMin)
	require.Equal(t, max, getMax)
}

func TestPairReset(t *testing.T) {
	pair := NewPair()

	min := int64(444)
	max := int64(66666)
	pair.min = min
	pair.max = max

	getMin, getMax := pair.Reset()

	require.Equal(t, min, getMin)
	require.Equal(t, max, getMax)

	require.Equal(t, int64(0), pair.min)
	require.Equal(t, int64(0), pair.max)
}
