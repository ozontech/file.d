package k8s

import (
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

func TestMultilineAction_Do(t *testing.T) {
	plugin := &MultilineAction{}
	config := &Config{
		SplitEventSize: predictionLookahead * 4,
	}
	plugin.Start(config, &pipeline.ActionPluginParams{Logger: zap.S(), PluginDefaultParams: &pipeline.PluginDefaultParams{
		PipelineSettings: &pipeline.Settings{
			MaxEventSize: 10,
		},
	}})

	item := &metaItem{
		nodeName:      "node_1",
		namespace:     "sre",
		podName:       "advanced-logs-checker-1111111111-trtrq",
		containerName: "duty-bot",
		containerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	meta := getLogFilename("k8s", item)
	putMeta(getPodInfo(item, true))

	tcs := []struct {
		Name       string
		EventParts []string

		ActionResults []pipeline.ActionResult
		ExpectedRoot  string
	}{
		{
			Name: "ok",
			// these parts are longer than max_event_size (5+26+7=38 vs 10), but they will be passed
			// because the buffer size check happens before adding to it
			EventParts:    []string{`{"log": "hello"}`, `{"log": "  "}`, `{"log": "world\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"hello  world\n","k8s_node":"","k8s_namespace":"sre","k8s_pod":"advanced-logs-checker-1111111111-trtrq","k8s_container":"duty-bot","k8s_pod_label_allowed_label":"allowed_value"}`,
		},
		{
			Name:          "continue process events",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other "}`, `{"log": "logs\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"some other logs\n","k8s_node":"","k8s_namespace":"sre","k8s_pod":"advanced-logs-checker-1111111111-trtrq","k8s_container":"duty-bot","k8s_pod_label_allowed_label":"allowed_value"}`,
		},
		{
			Name:          "must discard long event",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionDiscard},
		},
		{
			Name: "continue process events 2",
			// these parts are longer than max_event_size (5+26+7=38 vs 10), but they will be passed
			// because the buffer size check happens before adding to it
			EventParts:    []string{`{"log": "some "}`, `{"log": "other long long long long "}`, `{"log": "event\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"some other long long long long event\n","k8s_node":"","k8s_namespace":"sre","k8s_pod":"advanced-logs-checker-1111111111-trtrq","k8s_container":"duty-bot","k8s_pod_label_allowed_label":"allowed_value"}`,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			root := insaneJSON.Spawn()
			for i, part := range tc.EventParts {
				require.NoError(t, root.DecodeString(part))
				event := &pipeline.Event{Root: root, SourceName: meta, Size: len(part)}

				result := plugin.Do(event)

				assert.Equalf(t, tc.ActionResults[i], result, "wrong action result for iteration=%v, part=%s", i, part)
			}
			if tc.ActionResults[len(tc.ActionResults)-1] != pipeline.ActionDiscard {
				assert.Equal(t, tc.ExpectedRoot, root.EncodeToString())
			}
		})
	}
}
