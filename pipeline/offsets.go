package pipeline

type Offsets struct {
	current       int64
	streamOffsets SliceMap
}

func NewOffsets(current int64, streamOffsets SliceMap) Offsets {
	return Offsets{
		current:       current,
		streamOffsets: streamOffsets,
	}
}

func (o Offsets) byStream(stream string) int64 {
	offset, found := o.streamOffsets.Get(StreamName(stream))
	if !found {
		return -1
	}
	return offset
}
