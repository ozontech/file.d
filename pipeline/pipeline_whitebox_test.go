package pipeline

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestPipeline_streamEvent(t *testing.T) {
	settings := &Settings{
		Capacity: 5,
		Decoder:  "json",
	}
	p := New("test", settings, prometheus.NewRegistry())

	streamID := StreamID(123123)
	procs := int32(7)
	p.procCount = atomic.NewInt32(procs)
	p.input = &TestInputPlugin{}
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

// Can't use fake plugin here dye cycle import
type TestInputPlugin struct{}

func (p *TestInputPlugin) Start(_ AnyConfig, _ *InputPluginParams) {}
func (p *TestInputPlugin) Stop()                                   {}
func (p *TestInputPlugin) Commit(_ *Event)                         {}
func (p *TestInputPlugin) PassEvent(_ *Event) bool {
	return true
}
