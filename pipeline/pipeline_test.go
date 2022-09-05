package pipeline_test

import (
	"reflect"
	"testing"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/stretchr/testify/require"
)

func getFakeInputInfo() *pipeline.InputPluginInfo {
	input, _ := fake.Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "",
			Factory: nil,
			Config:  nil,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "",
		},
	}
}

func TestInUnparsableMessages(t *testing.T) {
	metric.InitStats()

	name := "invalid_json"
	message := []byte("{wHo Is Json: YoU MeAn SoN oF JoHn???")
	pipelineSettings := &pipeline.Settings{
		Capacity: 5,
		Decoder:  "json",
	}
	offset := int64(666)
	sourceID := pipeline.SourceID(3<<16 + int(10))

	t.Run(name, func(t *testing.T) {
		pipe := pipeline.New("test_pipeline", pipelineSettings, nil)

		pipe.SetInput(getFakeInputInfo())

		seqID := pipe.In(sourceID, "kafka", offset, message, false, nil)
		require.Equal(t, pipeline.EventSeqIDError, seqID)

		refPipe := reflect.ValueOf(pipe)
		eventPool := reflect.Indirect(refPipe).FieldByName("eventPool")

		free1slice := reflect.
			Indirect(
				reflect.
					Indirect(eventPool).
					FieldByName("free1")).
			Slice(0, pipelineSettings.Capacity)
		free2slice := reflect.
			Indirect(
				reflect.
					Indirect(eventPool).
					FieldByName("free2")).
			Slice(0, pipelineSettings.Capacity)

		for i := 0; i < pipelineSettings.Capacity; i++ {
			// free1, free2 are []atomic.Bool which underlying v is really uint32
			// so if v val == uint32(1) event was released.
			free1idxUint := reflect.
				Indirect(free1slice.Index(i)).
				FieldByName("v").
				Uint()
			require.EqualValues(t, uint32(1), free1idxUint)

			free2idxUint := reflect.
				Indirect(free2slice.Index(i)).
				FieldByName("v").
				Uint()
			require.EqualValues(t, uint32(1), free2idxUint)
		}
	})
}

func TestInInvalidMessages(t *testing.T) {
	metric.InitStats()

	cases := []struct {
		name             string
		message          []byte
		pipelineSettings *pipeline.Settings
		offset           int64
		sourceID         pipeline.SourceID
	}{
		{
			name:    "empty_message",
			message: []byte(""),
			pipelineSettings: &pipeline.Settings{
				Capacity: 5,
				Decoder:  "json",
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(1<<16 + int(1)),
		},
		{
			name:    "too_long_message",
			message: []byte("{\"value\":\"i'm longer than 1 byte\""),
			pipelineSettings: &pipeline.Settings{
				Capacity:     5,
				Decoder:      "json",
				MaxEventSize: 1,
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(2<<16 + int(3)),
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			pipe := pipeline.New("test_pipeline", tCase.pipelineSettings, nil)

			pipe.SetInput(getFakeInputInfo())

			seqID := pipe.In(tCase.sourceID, "kafka", tCase.offset, tCase.message, false, nil)
			require.Equal(t, pipeline.EventSeqIDError, seqID)
		})
	}
}

func TestSkipOldOffsets(t *testing.T) {
	metric.InitStats()

	messageStdErrCommitted := []byte(`{"stream":"stderr","message":"gogogo"}`)
	messageStdOutCommitted := []byte(`{"stream":"stdout","message":"i was committed"}`)
	messageStdErrNOTCommittedPart1 := []byte(`{"stream":"stderr","message":"i wasn't committed, pod were killed"}`)
	messageStdErrNOTCommittedPart2 := []byte(`{"stream":"stderr","message":"me either %("}`)
	messageStdOutCommittedLast := []byte(`{"stream":"stdout","message":"final stdout"}`)
	messageStdOutNotCommitted := []byte(`{"stream":"stdout","message":"commit me please"}`)

	stdErrStartPos := int64(len(messageStdErrCommitted))

	offsetsMap := pipeline.SliceFromMap(map[pipeline.StreamName]int64{
		"stderr": stdErrStartPos,
		"stdout": stdErrStartPos +
			int64(len(messageStdOutCommitted)) +
			int64(len(messageStdErrNOTCommittedPart1)) +
			int64(len(messageStdErrNOTCommittedPart2)) +
			int64(len(messageStdOutCommittedLast)),
	})

	pipelineSettings := &pipeline.Settings{Capacity: 5, Decoder: "json", StreamField: "stream"}
	sourceID := pipeline.SourceID(1<<16 + int(1))
	pipe := pipeline.New("test_pipeline", pipelineSettings, nil)
	pipe.SetInput(getFakeInputInfo())

	seqID := pipe.In(sourceID, "file_in", stdErrStartPos+int64(len(messageStdOutCommitted)), messageStdOutCommitted, false, offsetsMap)
	// First stdout event already written. Skip it.
	require.Equal(t, pipeline.EventSeqIDError, seqID)

	seqID = pipe.In(sourceID, "file_in",
		stdErrStartPos+
			int64(len(messageStdOutCommitted))+
			int64(len(messageStdErrNOTCommittedPart1)),
		messageStdErrNOTCommittedPart1,
		false,
		offsetsMap)
	// Stderr msg that weren't committed.
	require.Equal(t, uint64(1), seqID)

	seqID = pipe.In(sourceID, "file_in",
		stdErrStartPos+
			int64(len(messageStdOutCommitted))+
			int64(len(messageStdErrNOTCommittedPart1))+
			int64(len(messageStdErrNOTCommittedPart2)),
		messageStdErrNOTCommittedPart2,
		false,
		offsetsMap)
	// Second part of stderr msg that weren't committed.
	require.Equal(t, uint64(2), seqID)

	seqID = pipe.In(sourceID, "file_in",
		stdErrStartPos+
			int64(len(messageStdOutCommitted))+
			int64(len(messageStdErrNOTCommittedPart1))+
			int64(len(messageStdErrNOTCommittedPart2))+
			int64(len(messageStdOutCommittedLast)),
		messageStdOutCommittedLast,
		false,
		offsetsMap)
	// Last committed stdout message. Skip.
	require.Equal(t, uint64(0), seqID)

	seqID = pipe.In(sourceID, "file_in",
		stdErrStartPos+
			int64(len(messageStdOutCommitted))+
			int64(len(messageStdErrNOTCommittedPart1))+
			int64(len(messageStdErrNOTCommittedPart2))+
			int64(len(messageStdOutCommittedLast))+
			int64(len(messageStdOutNotCommitted)),
		messageStdOutNotCommitted,
		false,
		offsetsMap)
	// Last committed stdout message. Skip.
	require.Equal(t, uint64(1), seqID)
}
