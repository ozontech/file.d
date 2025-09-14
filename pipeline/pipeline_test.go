package pipeline_test

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

func TestInInvalidMessages(t *testing.T) {
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
				Capacity:           5,
				Decoder:            "json",
				MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(1<<16 + int(1)),
		},
		{
			name:    "too_long_message",
			message: []byte("{\"value\":\"i'm longer than 1 byte\""),
			pipelineSettings: &pipeline.Settings{
				Capacity:           5,
				Decoder:            "json",
				MaxEventSize:       1,
				MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(2<<16 + int(3)),
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			pipe := pipeline.New("test_pipeline", tCase.pipelineSettings, prometheus.NewRegistry(), zap.NewNop())

			pipe.SetInput(getFakeInputInfo())

			seqID := pipe.In(tCase.sourceID, "kafka", test.NewOffset(tCase.offset), tCase.message, false, nil)
			require.Equal(t, pipeline.EventSeqIDError, seqID)
		})
	}
}

func BenchmarkMetaTemplater(b *testing.B) {
	pipelineSettings := &pipeline.Settings{
		Capacity:           b.N,
		Decoder:            "cri",
		MetricHoldDuration: pipeline.DefaultMetricHoldDuration,
	}

	pipe := pipeline.New("test_pipeline", pipelineSettings, prometheus.NewRegistry(), zap.NewNop())
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
			test.NewOffset(int64(i)),
			[]byte("2016-10-06T00:17:09.669794202Z stdout P partial content 1\n"),
			false,
			metadata.MetaData{},
		)
	}
}
