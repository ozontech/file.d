package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractParams(t *testing.T) {
	index := 123456789
	partition := int32(123)

	x := makeSourceID(index, partition)

	newIndex, newPartition := extractMessageParams(x)

	assert.Equal(t, index, newIndex, "values aren't equal")
	assert.Equal(t, partition, newPartition, "values aren't equal")
}
