package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAssembleSourceID(t *testing.T) {
	index := 123456789
	partition := int32(123)

	x := assembleSourceID(index, partition)

	newIndex, newPartition := disassembleSourceID(x)

	assert.Equal(t, index, newIndex, "values aren't equal")
	assert.Equal(t, partition, newPartition, "values aren't equal")
}

func TestAssembleOffset(t *testing.T) {
	message := &kgo.Record{
		LeaderEpoch: 27,
		Offset:      237582035700,
	}
	x := assembleOffset(message)

	epochOffset := disassembleOffset(x)

	assert.Equal(t, message.LeaderEpoch, epochOffset.Epoch, "values aren't equal")
	assert.Equal(t, message.Offset+1, epochOffset.Offset, "values aren't equal")
}
