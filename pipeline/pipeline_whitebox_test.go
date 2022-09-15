package pipeline

import (
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

func TestPipeline_streamEvent(t *testing.T) {
	metric.InitStats()

	settings := &Settings{
		Capacity: 5,
		Decoder:  "json",
	}
	p := New("test", settings, nil)

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

func (p *TestInputPlugin) Start(config AnyConfig, params *InputPluginParams) {}
func (p *TestInputPlugin) Stop()                                             {}
func (p *TestInputPlugin) Commit(*Event)                                     {}
func (p *TestInputPlugin) PassEvent(event *Event) bool {
	return true
}
