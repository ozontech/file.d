package k8s

import (
	"fmt"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultilineAction_Do(t *testing.T) {
	plugin := &MultilineAction{}
	config := &Config{
		SplitEventSize: predictionLookahead * 4,
	}
	params := test.NewEmptyActionPluginParams()
	params.PipelineSettings = &pipeline.Settings{MaxEventSize: 20}
	plugin.Start(config, params)

	item := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1111111111-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	filename := getLogFilename("k8s", item)
	meta.PutMeta(getPodInfo(item, true))
	meta.SelfNodeName = "node_1"
	meta.NodeLabels = map[string]string{"zone": "z34"}

	tcs := []struct {
		Name       string
		EventParts []string

		CutOffEventByLimit      bool
		CutOffEventByLimitField string

		ActionResults []pipeline.ActionResult
		ExpectedRoot  string
	}{
		{
			Name: "ok",
			// these parts are longer than max_event_size (5+2+7=15 vs 10), but they will be passed
			// because the buffer size check happens before adding to it
			EventParts:    []string{`{"log": "hello"}`, `{"log": "  "}`, `{"log": "world\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  wrapK8sInfo(`hello  world\n`, item, meta.SelfNodeName),
		},
		{
			Name:          "continue process events",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other "}`, `{"log": "logs\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  wrapK8sInfo(`some other logs\n`, item, meta.SelfNodeName),
		},
		{
			Name:          "must discard long event",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionDiscard},
		},
		{
			Name:          "continue process events 2",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other long long long long "}`, `{"log": "event\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionDiscard},
			ExpectedRoot:  wrapK8sInfo(`event\n`, item, meta.SelfNodeName),
		},
		{
			Name:               "must cutoff long event",
			EventParts:         []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			CutOffEventByLimit: true,
			ActionResults:      []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:       wrapK8sInfo(`some other long l\n`, item, meta.SelfNodeName),
		},
		{
			Name:                    "must cutoff long event with field",
			EventParts:              []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			CutOffEventByLimit:      true,
			CutOffEventByLimitField: "cutoff",
			ActionResults:           []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot: fmt.Sprintf(
				`{"log":"%s","k8s_pod":"%s","k8s_namespace":"%s","k8s_container":"%s","k8s_container_id":"%s","k8s_node":"%s","k8s_pod_label_allowed_label":"allowed_value","k8s_node_label_zone":"z34","cutoff":true}`,
				`some other long l\n`, item.PodName, item.Namespace, item.ContainerName, item.ContainerID, meta.SelfNodeName,
			),
		},
	}
	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			for i, part := range tc.EventParts {
				plugin.cutOffEventByLimit = tc.CutOffEventByLimit
				plugin.cutOffEventByLimitField = tc.CutOffEventByLimitField

				require.NoError(t, root.DecodeString(part))
				event := &pipeline.Event{Root: root, SourceName: filename, Size: len(part)}

				pipeline.CreateNestedField(event.Root, []string{"k8s_pod"}).MutateToString(string(item.PodName))
				pipeline.CreateNestedField(event.Root, []string{"k8s_namespace"}).MutateToString(string(item.Namespace))
				pipeline.CreateNestedField(event.Root, []string{"k8s_container"}).MutateToString(string(item.ContainerName))
				pipeline.CreateNestedField(event.Root, []string{"k8s_container_id"}).MutateToString(string(item.ContainerID))

				result := plugin.Do(event)

				assert.Equalf(t, tc.ActionResults[i], result, "wrong action result for iteration=%v, part=%s", i, part)
			}
			if tc.ActionResults[len(tc.ActionResults)-1] != pipeline.ActionDiscard {
				assert.Equal(t, tc.ExpectedRoot, root.EncodeToString())
			}
		})
	}
}

func TestMultilineAction_Do_shouldSplit(t *testing.T) {
	plugin := &MultilineAction{}
	config := &Config{
		SplitEventSize: predictionLookahead * 4,
	}
	plugin.Start(config, test.NewEmptyActionPluginParams())

	item := &meta.MetaItem{
		Namespace:     "sre",
		PodName:       "advanced-logs-checker-1111111111-trtrq",
		ContainerName: "duty-bot",
		ContainerID:   "4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0",
	}
	filename := getLogFilename("k8s", item)
	meta.PutMeta(getPodInfo(item, true))

	var (
		splitEventSize = predictionLookahead * 2
		longChunk      = strings.Repeat("a", splitEventSize*2)
		superLongChunk = strings.Repeat("a", predictionLookahead)
	)

	tcs := []struct {
		Name             string
		MaxEventSize     int
		Message          string
		ExpectedLogField string
		ActionResult     pipeline.ActionResult
	}{
		{
			Name:             "is last chunk and shouldSplit is false",
			MaxEventSize:     1,
			Message:          wrapLogContent(`hi\n`),
			ExpectedLogField: `"hi\n"`,
			ActionResult:     pipeline.ActionPass,
		},
		{
			Name:             "is last chunk and shouldSplit is true",
			MaxEventSize:     2,
			Message:          wrapLogContent(longChunk + `\n`),
			ExpectedLogField: fmt.Sprintf(`"%s\n"`, longChunk),
			ActionResult:     pipeline.ActionPass,
		},
		{
			Name:             "isnt last chunk and shouldSplit is true",
			MaxEventSize:     4,
			Message:          wrapLogContent(longChunk),
			ExpectedLogField: fmt.Sprintf(`"%s"`, longChunk),
			ActionResult:     pipeline.ActionPass,
		},
		{
			Name:             "isnt last chunk and shouldSplit is false",
			MaxEventSize:     10,
			Message:          wrapLogContent(`hi`),
			ExpectedLogField: `"hi"`,
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "end prev event",
			MaxEventSize:     10,
			Message:          wrapLogContent(`.\n`),
			ExpectedLogField: `"hi.\n"`,
			ActionResult:     pipeline.ActionPass,
		},
		// test max_log_size for chunks
		{
			Name:             "text log size too small chunk1",
			MaxEventSize:     2,
			Message:          wrapLogContent(`chunk1`),
			ExpectedLogField: `"chunk1"`,
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "text log size too small chunk2",
			MaxEventSize:     2,
			Message:          wrapLogContent(`chunk2`),
			ExpectedLogField: `"chunk2"`,
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "text log size too small chunk3",
			MaxEventSize:     2,
			Message:          wrapLogContent(`chunk3\n`),
			ExpectedLogField: `"chunk3\n"`,
			ActionResult:     pipeline.ActionDiscard,
		},
		// test max_log_size = 0
		{
			Name:             "max_log_size is 0 chunk1",
			Message:          wrapLogContent(`chunk1`),
			ExpectedLogField: `"chunk1"`,
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "max_log_size is 0 chunk2",
			Message:          wrapLogContent(`chunk2`),
			ExpectedLogField: `"chunk2"`,
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "max_log_size is 0 chunk3",
			Message:          wrapLogContent(`chunk3\n`),
			ExpectedLogField: `"chunk1chunk2chunk3\n"`,
			ActionResult:     pipeline.ActionPass,
		},
		// test max_log_size = 5 and it is long last chunk
		{
			Name:             "long last chunk",
			MaxEventSize:     5,
			Message:          wrapLogContent(longChunk),
			ExpectedLogField: fmt.Sprintf(`"%s"`, longChunk),
			// it is ok because we will skip this event in the `pipeline.In` method
			ActionResult: pipeline.ActionPass,
		},
		// even_size > split_event_size > max_event_size
		{
			Name:             "event_size super large 1",
			MaxEventSize:     predictionLookahead + len(`""`),
			Message:          wrapLogContent(superLongChunk),
			ExpectedLogField: fmt.Sprintf(`"%s"`, superLongChunk),
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "event_size super large 2",
			MaxEventSize:     predictionLookahead + len(`""`),
			Message:          wrapLogContent(superLongChunk),
			ExpectedLogField: fmt.Sprintf(`"%s"`, superLongChunk),
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "event_size super large 3",
			MaxEventSize:     predictionLookahead + len(`""`),
			Message:          wrapLogContent(superLongChunk),
			ExpectedLogField: fmt.Sprintf(`"%s"`, superLongChunk),
			ActionResult:     pipeline.ActionCollapse,
		},
		{
			Name:             "event_size super large 4",
			MaxEventSize:     predictionLookahead + len(`""`),
			Message:          wrapLogContent(superLongChunk + `\n`),
			ExpectedLogField: fmt.Sprintf(`"%s\n"`, strings.Repeat(superLongChunk, 1)),
			ActionResult:     pipeline.ActionDiscard,
		},
	}

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			plugin.maxEventSize = tc.MaxEventSize
			require.NoError(t, root.DecodeString(tc.Message))
			event := &pipeline.Event{Root: root, SourceName: filename, Size: len(tc.Message)}
			pipeline.CreateNestedField(event.Root, []string{"k8s_pod"}).MutateToString(string(item.PodName))
			pipeline.CreateNestedField(event.Root, []string{"k8s_namespace"}).MutateToString(string(item.Namespace))
			pipeline.CreateNestedField(event.Root, []string{"k8s_container"}).MutateToString(string(item.ContainerName))
			pipeline.CreateNestedField(event.Root, []string{"k8s_container_id"}).MutateToString(string(item.ContainerID))

			result := plugin.Do(event)
			resultRoot := root.Dig("log").EncodeToString()

			assert.Equalf(t, tc.ActionResult, result, "wrong action result")
			assert.Equal(t, len(tc.ExpectedLogField), len(resultRoot))
			assert.Equal(t, tc.ExpectedLogField, resultRoot)
		})
	}
}

func wrapLogContent(s string) string {
	return fmt.Sprintf(`{"log": "%s"}`, s)
}

func wrapK8sInfo(s string, item *meta.MetaItem, nodeName string) string {
	return fmt.Sprintf(
		`{"log":"%s","k8s_pod":"%s","k8s_namespace":"%s","k8s_container":"%s","k8s_container_id":"%s","k8s_node":"%s","k8s_pod_label_allowed_label":"allowed_value","k8s_node_label_zone":"z34"}`,
		s, item.PodName, item.Namespace, item.ContainerName, item.ContainerID, nodeName)
}
