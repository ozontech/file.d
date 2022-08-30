package pipeline

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeltaWrapper(t *testing.T) {
	dw := newDeltaWrapper()
	assert.Equal(t, int64(0), dw.get())

	initial := int64(0)

	for i := 0; i <= 1000000; i++ {
		delta := rand.Int63n(5000)
		initial += delta
		assert.Equal(t, delta, int64(dw.updateValue(initial)))
		assert.Equal(t, initial, dw.get())
	}
}
