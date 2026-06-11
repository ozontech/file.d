package file_multiline

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlugin_Do(t *testing.T) {
	plugin := &Plugin{}
	config := &Config{
		SplitEventSize: predictionLookahead * 4,
	}
	params := test.NewEmptyActionPluginParams()
	params.PipelineSettings = &pipeline.Settings{MaxEventSize: 20}
	plugin.Start(config, params)

	sourceName := "/var/log/containers/app.log"

	tcs := []struct {
		Name       string
		EventParts []string

		CutOffEventByLimit      bool
		CutOffEventByLimitField string

		ActionResults []pipeline.ActionResult
		ExpectedRoot  string
	}{
		{
			Name:          "ok",
			EventParts:    []string{`{"log": "hello"}`, `{"log": "  "}`, `{"log": "world\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"hello  world\n"}`,
		},
		{
			Name:          "continue process events",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other "}`, `{"log": "logs\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"some other logs\n"}`,
		},
		{
			Name: "literal backslash-r-n suffix is not record end",
			EventParts: []string{
				`{"log": "chunk1"}`,
				`{"log": "OAA\\r\\n"}`,
				`{"log": "chunk2\n"}`,
			},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:  `{"log":"chunk1OAA\\r\\nchunk2\n"}`,
		},
		{
			Name:          "must discard long event",
			EventParts:    []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			ActionResults: []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionDiscard},
		},
		{
			Name:               "must cutoff long event",
			EventParts:         []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			CutOffEventByLimit: true,
			ActionResults:      []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:       `{"log":"some other long l\n"}`,
		},
		{
			Name:                    "must cutoff long event with field",
			EventParts:              []string{`{"log": "some "}`, `{"log": "other long "}`, `{"log":"long long"}`, `{"log": "event\n"}`},
			CutOffEventByLimit:      true,
			CutOffEventByLimitField: "cutoff",
			ActionResults:           []pipeline.ActionResult{pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionCollapse, pipeline.ActionPass},
			ExpectedRoot:            `{"log":"some other long l\n","cutoff":true}`,
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
				event := &pipeline.Event{Root: root, SourceName: sourceName, Size: len(part)}

				result := plugin.Do(event)

				assert.Equalf(t, tc.ActionResults[i], result, "wrong action result for iteration=%v, part=%s", i, part)
			}
			if tc.ActionResults[len(tc.ActionResults)-1] != pipeline.ActionDiscard {
				assert.Equal(t, tc.ExpectedRoot, root.EncodeToString())
			}
		})
	}
}

func TestPlugin_Do_shouldSplit(t *testing.T) {
	plugin := &Plugin{}
	config := &Config{
		SplitEventSize: predictionLookahead * 4,
	}
	plugin.Start(config, test.NewEmptyActionPluginParams())

	sourceName := "/var/log/containers/app.log"

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
			Name:             "isnt last chunk and shouldSplit is false",
			MaxEventSize:     10,
			Message:          wrapLogContent(`hi`),
			ExpectedLogField: `"hi"`,
			ActionResult:     pipeline.ActionCollapse,
		},
	}

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			plugin.maxEventSize = tc.MaxEventSize
			require.NoError(t, root.DecodeString(tc.Message))
			event := &pipeline.Event{Root: root, SourceName: sourceName, Size: len(tc.Message)}

			result := plugin.Do(event)
			resultRoot := root.Dig("log").EncodeToString()

			assert.Equalf(t, tc.ActionResult, result, "wrong action result")
			assert.Equal(t, tc.ExpectedLogField, resultRoot)
		})
	}
}

func wrapLogContent(s string) string {
	return fmt.Sprintf(`{"log": "%s"}`, s)
}
