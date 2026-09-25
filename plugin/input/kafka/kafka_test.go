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
	message := &kgo.Record{
		LeaderEpoch: 27,
		Offset:      237582035700,
	}
	x := assembleOffset(message)

	epochOffset := disassembleOffset(x)

	assert.Equal(t, message.LeaderEpoch, epochOffset.Epoch, "values aren't equal")
	assert.Equal(t, message.Offset+1, epochOffset.Offset, "values aren't equal")
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
		want string
	}{
		{
			name: "basic",
			rec:  &kgo.Record{Topic: "topic", Partition: 1, Offset: 100},
			want: metadata.Hash([]byte("topic"), []byte("1"), []byte("100")),
		},
		{
			name: "zero values",
			rec:  &kgo.Record{Topic: "", Partition: 0, Offset: 0},
			want: metadata.Hash([]byte(""), []byte("0"), []byte("0")),
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
