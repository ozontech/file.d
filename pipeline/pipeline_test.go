package pipeline_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

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
				Capacity: 5,
				Decoder:  "json",
				Metric: &pipeline.MetricSettings{
					HoldDuration: pipeline.DefaultMetricHoldDuration,
				},
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
				Metric: &pipeline.MetricSettings{
					HoldDuration: pipeline.DefaultMetricHoldDuration,
				},
			},
			offset:   int64(666),
			sourceID: pipeline.SourceID(2<<16 + int(3)),
		},
	}

	for _, tCase := range cases {
		t.Run(tCase.name, func(t *testing.T) {
			t.Parallel()

			pipe := pipeline.New("test_pipeline", tCase.pipelineSettings, prometheus.NewRegistry(), zap.NewNop())

			pipe.SetInput(getFakeInputInfo())

			seqID := pipe.In(tCase.sourceID, "kafka", test.NewOffset(tCase.offset), tCase.message, false, nil)
			require.Equal(t, pipeline.EventSeqIDError, seqID)
		})
	}
}

func TestAntispamCRI(t *testing.T) {
	formatCri := func(flag, data string) []byte {
		return fmt.Appendf(nil, "2026-06-06T00:00:00.123456789Z stdout %s %s\n", flag, data)
	}

	type iter struct {
		sourceID pipeline.SourceID
		input    []byte
		banned   bool
	}

	settings := &pipeline.Settings{
		Decoder:  "cri",
		Capacity: 10,
		Metric: &pipeline.MetricSettings{
			HoldDuration: pipeline.DefaultMetricHoldDuration,
		},
		Antispam: pipeline.AntispamSettings{
			Threshold:           3,
			MaintenanceInterval: time.Second,
		},
	}

	cases := []struct {
		name     string
		settings *pipeline.Settings
		iters    []iter
	}{
		{
			name:     "only_full",
			settings: settings,
			iters: []iter{
				{
					sourceID: 1,
					input:    formatCri("F", "test1"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test2"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test3"),
					banned:   true,
				},
			},
		},
		{
			name:     "only_partial",
			settings: settings,
			iters: []iter{
				{
					sourceID: 1,
					input:    formatCri("P", "test1_1"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test1_2"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test1_3"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test1_4"),
				},
			},
		},
		{
			name:     "partial_passed",
			settings: settings,
			iters: []iter{
				{
					sourceID: 1,
					input:    formatCri("F", "test1"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test2_1"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test2_2"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test2_3"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test3"),
					banned:   true,
				},
			},
		},
		{
			name:     "partial_banned",
			settings: settings,
			iters: []iter{
				{
					sourceID: 1,
					input:    formatCri("F", "test1"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test2"),
				},
				{
					sourceID: 1,
					input:    formatCri("P", "test3_1"),
					banned:   true,
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test3_2"),
					banned:   true,
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test4"),
					banned:   true,
				},
			},
		},
		{
			name:     "multi_sources",
			settings: settings,
			iters: []iter{
				{
					sourceID: 1,
					input:    formatCri("F", "test1"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test2"),
				},
				{
					sourceID: 2,
					input:    formatCri("F", "test1"),
				},
				{
					sourceID: 1,
					input:    formatCri("F", "test3"),
					banned:   true,
				},
				{
					sourceID: 2,
					input:    formatCri("P", "test2_1"),
				},
				{
					sourceID: 2,
					input:    formatCri("P", "test2_2"),
				},
				{
					sourceID: 2,
					input:    formatCri("F", "test2_3"),
				},
				{
					sourceID: 2,
					input:    formatCri("P", "test3_1"),
					banned:   true,
				},
				{
					sourceID: 2,
					input:    formatCri("F", "test3_2"),
					banned:   true,
				},
			},
		},
	}

	for i, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pipe := pipeline.New(fmt.Sprintf("test_pipeline%d", i), tt.settings, prometheus.NewRegistry(), zap.NewNop())
			pipe.DisableStreams()

			for j, iter := range tt.iters {
				seqID := pipe.In(iter.sourceID, strconv.Itoa(int(iter.sourceID)), test.NewOffset(int64(j)+1), iter.input, false, nil)
				require.Equal(t, iter.banned, seqID == pipeline.EventSeqIDError)
			}
		})
	}
}

func BenchmarkMetaTemplater(b *testing.B) {
	pipelineSettings := &pipeline.Settings{
		Capacity: b.N,
		Decoder:  "cri",
		Metric: &pipeline.MetricSettings{
			HoldDuration: pipeline.DefaultMetricHoldDuration,
		},
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

	for i := range b.N {
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
