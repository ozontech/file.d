package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAssembleSourceID(t *testing.T) {
	index := 123456789
	partition := int32(123)

	x := assembleSourceID(index, partition)

	newIndex, newPartition := disassembleSourceID(x)

	assert.Equal(t, index, newIndex, "values aren't equal")
	assert.Equal(t, partition, newPartition, "values aren't equal")
}
