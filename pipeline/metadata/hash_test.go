package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts [][]byte
	}{
		{
			name:  "single part",
			parts: [][]byte{[]byte("hello")},
		},
		{
			name:  "multiple parts",
			parts: [][]byte{[]byte("hello"), []byte("world"), []byte("foo")},
		},
		{
			name:  "empty",
			parts: [][]byte{},
		},
		{
			name:  "empty parts",
			parts: [][]byte{[]byte(""), []byte("a")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := Hash(tt.parts...)
			assert.NotEmpty(t, result)
		})
	}
}

func TestFastHashDeterministic(t *testing.T) {
	t.Parallel()

	assert.Equal(t,
		Hash([]byte("a"), []byte("b")),
		Hash([]byte("a"), []byte("b")),
		"same inputs should produce the same hash",
	)
}

func TestFastHashDiffers(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		Hash([]byte("a")),
		Hash([]byte("b")),
		"different inputs should produce different hashes",
	)
}
