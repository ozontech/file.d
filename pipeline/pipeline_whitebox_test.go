package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestPipeline_streamEvent(t *testing.T) {

	settings := &Settings{
		Capacity: 5,
		Decoder:  "json",
	}
	p := New("test", settings, nil)

	streamID := StreamID(123123)
	procs := int32(7)
	p.procCount = atomic.NewInt32(procs)

	event := newEvent()
	event.SourceID = SourceID(streamID)
	event.streamName = DefaultStreamName
	event.SeqID = 123456789

	p.streamEvent(event)

	assert.Equal(t, event, p.streamer.getStream(streamID, DefaultStreamName).first)

	p.UseSpread()
	p.streamEvent(event)

	expectedStreamID := StreamID(event.SeqID % uint64(procs))

	assert.Equal(t, event, p.streamer.getStream(expectedStreamID, DefaultStreamName).first)
}
