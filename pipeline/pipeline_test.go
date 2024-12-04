package pipeline_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
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
	name := "invalid_json"
	message := []byte("{wHo Is Json: YoU MeAn SoN oF JoHn???")
	pipelineSettings := &pipeline.Settings{
		Capacity:           5,
		Decoder:            "json",
		MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
	}
	offset := int64(666)
	sourceID := pipeline.SourceID(3<<16 + int(10))

	t.Run(name, func(t *testing.T) {
		pipe := pipeline.New("test_pipeline", pipelineSettings, prometheus.NewRegistry())

		pipe.SetInput(getFakeInputInfo())

		seqID := pipe.In(sourceID, "kafka", test.Offset(offset), message, false, nil)
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
				FieldByName("v").
				Uint()
			require.EqualValues(t, uint32(1), free1idxUint)

			free2idxUint := reflect.
				Indirect(free2slice.Index(i)).
				FieldByName("v").
				FieldByName("v").
				Uint()
			require.EqualValues(t, uint32(1), free2idxUint)
		}
	})
}

func BenchmarkMetaTemplater(b *testing.B) {
	pipelineSettings := &pipeline.Settings{
		Capacity:           b.N,
		Decoder:            "cri",
		MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
	}

	pipe := pipeline.New("test_pipeline", pipelineSettings, prometheus.NewRegistry())
	pipe.SetInput(getFakeInputInfo())
	plugin, config := devnull.Factory()
	outputPlugin := plugin.(*devnull.Plugin)

	pipe.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})

	for i := 0; i < b.N; i++ {
		rest := i % 100
		pipe.In(
			pipeline.SourceID(1<<16+rest),
			fmt.Sprintf(
				"/k8s-logs/advanced-logs-checker-1566485760-trtrq-%d_sre-%d_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0%d.log",
				rest, rest, rest,
			),
			test.Offset(i),
			[]byte("2016-10-06T00:17:09.669794202Z stdout P partial content 1\n"),
			false,
			metadata.MetaData{},
		)
	}
}
