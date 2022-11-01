package pipeline

import (
	"html/template"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPipelineHMTLTemplatesExists(t *testing.T) {
	t.Parallel()
	tplCases := []struct {
		name  string
		route string
	}{
		{
			name:  "start_page",
			route: StartPageHTML,
		},
		{
			name:  "pipeline_info",
			route: InfoPipelineHTML,
		},
	}

	for _, tplCase := range tplCases {
		t.Run(tplCase.name, func(t *testing.T) {
			tpl, err := template.ParseFS(PipelineTpl, tplCase.route)
			assert.NoErrorf(t, err, "err parsing html template")
			assert.NotNil(t, tpl)
		})
	}
}
