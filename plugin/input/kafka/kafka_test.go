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
	tests := []struct {
		name  string
		epoch int32
		off   int64
	}{
		{name: "regular epoch", epoch: 27, off: 237582035700},
		{name: "no epoch", epoch: -1, off: 237582035700},
		{name: "zero epoch", epoch: 0, off: 0},
		{name: "max epoch", epoch: 65534, off: 12345},
		{name: "high offset", epoch: 5, off: 1 << 40},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			message := &kgo.Record{
				LeaderEpoch: tt.epoch,
				Offset:      tt.off,
			}

			epochOffset := disassembleOffset(assembleOffset(message))

			assert.Equal(t, tt.epoch, epochOffset.Epoch, "epoch isn't equal")
			assert.Equal(t, tt.off+1, epochOffset.Offset, "offset isn't equal")
		})
	}
}
