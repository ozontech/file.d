package pipeline

import (
	"testing"

	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// Can't use fake plugin here dye cycle import
type TestInputPlugin struct{}

func (p *TestInputPlugin) Start(_ AnyConfig, _ *InputPluginParams) {}
func (p *TestInputPlugin) Stop()                                   {}
func (p *TestInputPlugin) Commit(_ *Event)                         {}
func (p *TestInputPlugin) PassEvent(_ *Event) bool {
	return true
}

func TestPipelineStreamEvent(t *testing.T) {
	settings := &Settings{
		Capacity:           5,
		Decoder:            "json",
		MetricHoldDuration: DefaultMetricHoldDuration,
	}
	p := New("test", settings, prometheus.NewRegistry(), zap.NewNop())

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

func TestCheckInputBytes(t *testing.T) {
	cases := []struct {
		name             string
		pipelineSettings *Settings
		input            []byte
		want             []byte
		wantCutoff       bool
		wantOk           bool
	}{
		{
			name: "empty_input",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
			},
			input:  []byte(""),
			wantOk: false,
		},
		{
			name: "only_newline",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
			},
			input:  []byte("\n"),
			wantOk: false,
		},
		{
			name: "too_long_input",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MaxEventSize:       1,
				MetricHoldDuration: DefaultMetricHoldDuration,
			},
			input:  []byte("i'm longer than 1 byte"),
			wantOk: false,
		},
		{
			name: "no_cutoff",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
				MaxEventSize:       20,
				CutOffEventByLimit: true,
			},
			input:  []byte("some loooooooog"),
			want:   []byte("some loooooooog"),
			wantOk: true,
		},
		{
			name: "cutoff_no_newline",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
				MaxEventSize:       10,
				CutOffEventByLimit: true,
			},
			input:      []byte("some loooooooog"),
			want:       []byte("some loooo"),
			wantCutoff: true,
			wantOk:     true,
		},
		{
			name: "cutoff_newline",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
				MaxEventSize:       10,
				CutOffEventByLimit: true,
			},
			input:      []byte("some loooooooog\n"),
			want:       []byte("some loooo\n"),
			wantCutoff: true,
			wantOk:     true,
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			pipe := New("test_pipeline", tCase.pipelineSettings, prometheus.NewRegistry(), zap.NewNop())

			data, cutoff, ok := pipe.checkInputBytes(tCase.input, "test", nil)

			assert.Equal(t, tCase.wantCutoff, cutoff)
			assert.Equal(t, tCase.wantOk, ok)
			if !tCase.wantOk {
				return
			}
			assert.Equal(t, tCase.want, data)
		})
	}
}

func TestCheckInputBytesMetric(t *testing.T) {
	cases := []struct {
		name             string
		pipelineSettings *Settings
		sourceName       string
		meta             metadata.MetaData
		want             map[string]float64
	}{
		{
			name: "from_source1",
			pipelineSettings: &Settings{
				Capacity:           5,
				Decoder:            "raw",
				MetricHoldDuration: DefaultMetricHoldDuration,
				MaxEventSize:       1,
			},
			sourceName: "test-source",
			meta: metadata.MetaData{
				"some-key": "some-value",
			},
			want: map[string]float64{
				"test-source": 1,
				"some-value":  0,
			},
		},
		{
			name: "from_source2",
			pipelineSettings: &Settings{
				Capacity:            5,
				Decoder:             "raw",
				MetricHoldDuration:  DefaultMetricHoldDuration,
				MaxEventSize:        1,
				SourceNameMetaField: "test",
			},
			sourceName: "test-source",
			meta: metadata.MetaData{
				"some-key": "some-value",
			},
			want: map[string]float64{
				"test-source": 1,
				"some-value":  0,
			},
		},
		{
			name: "from_meta",
			pipelineSettings: &Settings{
				Capacity:            5,
				Decoder:             "raw",
				MetricHoldDuration:  DefaultMetricHoldDuration,
				MaxEventSize:        1,
				SourceNameMetaField: "test",
			},
			sourceName: "test-source",
			meta: metadata.MetaData{
				"test": "some-value",
			},
			want: map[string]float64{
				"test-source": 0,
				"some-value":  1,
			},
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			pipe := New("test_pipeline", tCase.pipelineSettings, prometheus.NewRegistry(), zap.NewNop())

			pipe.checkInputBytes([]byte("some log"), tCase.sourceName, tCase.meta)

			for k, v := range tCase.want {
				require.Equal(t, v, testutil.ToFloat64(pipe.maxEventSizeExceededMetric.WithLabelValues(k)))
			}
		})
	}
}
