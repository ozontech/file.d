package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloneString(t *testing.T) {
	target := []byte("clone me")
	stringNoAlloc := ByteToStringUnsafe(target)

	result := CloneString(stringNoAlloc)

	require.Equal(t, stringNoAlloc, result)
	require.NotSame(t, stringNoAlloc, result)
}

func TestByteToStringUnsafe(t *testing.T) {
	target := []byte("clone me")
	stringNoAlloc := ByteToStringUnsafe(target)

	require.Equal(t, string(target), stringNoAlloc)

	// string mutates with slice.
	target[0] = 'b'
	require.Equal(t, target[0], stringNoAlloc[0])
}

func TestStringToByteUnsafe(t *testing.T) {
	target := "clone me"
	bytesNoAlloc := StringToByteUnsafe(target)

	require.Equal(t, target, string(bytesNoAlloc))
}

func TestTrimSpaceFunc(t *testing.T) {
	t.Run("space", func(t *testing.T) {
		require.True(t, TrimSpaceFunc(' '))
	})

	t.Run("not_trim", func(t *testing.T) {
		require.False(t, TrimSpaceFunc('a'))
	})
}
