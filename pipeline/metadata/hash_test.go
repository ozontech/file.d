package metadata

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastHash(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		parts []string
	}{
		{
			name:  "single part",
			parts: []string{"hello"},
		},
		{
			name:  "multiple parts",
			parts: []string{"hello", "world", "foo"},
		},
		{
			name:  "empty",
			parts: []string{},
		},
		{
			name:  "empty parts",
			parts: []string{"", "a"},
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
		Hash("a", "b"),
		Hash("a", "b"),
		"same inputs should produce the same hash",
	)
}

func TestFastHashDiffers(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t,
		Hash("a"),
		Hash("b"),
		"different inputs should produce different hashes",
	)
}
