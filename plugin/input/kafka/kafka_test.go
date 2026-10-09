package kafka

import (
	"testing"

	"github.com/ozontech/file.d/pipeline/metadata"
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

func TestMetaInformationGetData(t *testing.T) {
	mi := newMetaInformation(&kgo.Record{
		Topic:     "test-topic",
		Partition: 3,
		Offset:    100,
	})

	data := mi.GetData()
	assert.Equal(t, "test-topic", data["topic"])
	assert.Equal(t, int32(3), data["partition"])
	assert.Equal(t, int64(100), data["offset"])
}

func TestMetaInformationGetCacheKey(t *testing.T) {
	tests := []struct {
		name string
		rec  *kgo.Record
		want uint64
	}{
		{
			name: "basic",
			rec:  &kgo.Record{Topic: "topic", Partition: 1, Offset: 100},
			want: metadata.Hash("topic", "1", "100"),
		},
		{
			name: "zero values",
			rec:  &kgo.Record{Topic: "", Partition: 0, Offset: 0},
			want: metadata.Hash("", "0", "0"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mi := newMetaInformation(tt.rec)
			assert.Equal(t, tt.want, mi.GetCacheKey())
		})
	}
}

func TestMetaInformationCacheKeyUniqueness(t *testing.T) {
	a := newMetaInformation(&kgo.Record{Topic: "topic", Partition: 1, Offset: 100})
	b := newMetaInformation(&kgo.Record{Topic: "topic", Partition: 1, Offset: 100})
	c := newMetaInformation(&kgo.Record{Topic: "topic", Partition: 2, Offset: 100})

	assert.Equal(t, a.GetCacheKey(), b.GetCacheKey(), "identical records should have the same cache key")
	assert.NotEqual(t, a.GetCacheKey(), c.GetCacheKey(), "different partitions should produce different cache keys")
}
