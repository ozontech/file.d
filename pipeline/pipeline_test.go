package pipeline_test

import (
	"reflect"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/stats"
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

func TestInInvalidMessagesMessages(t *testing.T) {
	stats.InitStats()

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
		{
			name:    "invalid_json",
			message: []byte("{wHo Is Json: YoU MeAn SoN oF JaSoN???"),
			pipelineSettings: &pipeline.Settings{
				Capacity: 5,
				Decoder:  "json",
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(3<<16 + int(10)),
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			pipe := pipeline.New("test_pipeline", tCase.pipelineSettings, nil)

			pipe.SetInput(getFakeInputInfo())
			fakePlugin := pipe.GetInput().(*fake.Plugin)

			wg := &sync.WaitGroup{}
			wg.Add(1)
			fakePlugin.SetCommitFn(func(event *pipeline.Event) {
				wg.Done()
			})

			seqID := pipe.In(tCase.sourceID, "kafka", tCase.offset, tCase.message, false)
			require.Equal(t, pipeline.SkippedSeqID, seqID)

			wg.Wait()
			refPipe := reflect.ValueOf(pipe)
			eventPool := reflect.Indirect(refPipe).FieldByName("eventPool")

			free1slice := reflect.
				Indirect(
					reflect.
						Indirect(eventPool).
						FieldByName("free1")).
				Slice(0, tCase.pipelineSettings.Capacity)
			free2slice := reflect.
				Indirect(
					reflect.
						Indirect(eventPool).
						FieldByName("free2")).
				Slice(0, tCase.pipelineSettings.Capacity)

			for i := 0; i < tCase.pipelineSettings.Capacity; i++ {
				// free1, free2 are []atomic.Bool which underlaying v is really uint32
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
}
