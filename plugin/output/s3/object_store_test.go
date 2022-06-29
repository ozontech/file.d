package s3

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObjectStoreClientLimiterCanCreate(t *testing.T) {
	t.Run("can", func(t *testing.T) {
		limiter := NewObjectStoreClientLimiter(1)
		require.True(t, limiter.CanCreate())
	})
	t.Run("can't", func(t *testing.T) {
		limiter := NewObjectStoreClientLimiter(0)
		require.False(t, limiter.CanCreate())
	})
}

func TestObjectStoreClientIncrement(t *testing.T) {
	limiter := NewObjectStoreClientLimiter(1)
	require.Equal(t, 0, limiter.created)
	limiter.Increment()
	require.Equal(t, 1, limiter.created)
}
