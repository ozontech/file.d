package fd

import (
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

type pipeInfo struct {
	In      pipeline.InPluginObservabilityInfo
	Out     pipeline.OutPluginObservabilityInfo
	Changes pipeline.ChangesDTO
}

func (f *FileD) servePipelines() http.HandlerFunc {
	tmpl, parseFSErr := template.ParseFS(pipeline.PipelineTpl, pipeline.StartPageHTML)
	if parseFSErr != nil {
		logger.Errorf("can't parse html template: %s", parseFSErr.Error())
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		if parseFSErr != nil {
			_, _ = fmt.Fprintf(w, "<html><body>can't parse html: %s</body></html>", parseFSErr.Error())
			return
		}

		pipesInfo := []pipeInfo{}
		for _, pipe := range f.Pipelines {
			info := pipeInfo{}
			in, err := pipe.GetInput().GetObservabilityInfo()
			if err != nil {
				logger.Errorf("can't get pipeline observability info: %s", err.Error())
				_, _ = fmt.Fprintf(w, "<html><body>can't get pipeline observability info: %s</body></html>", err.Error())
				return
			}
			info.In = in
			info.Out = pipe.GetOutput().GetObservabilityInfo()
			info.Changes = pipe.GetChanges(pipe.GetDeltas())
			info.Changes.Interval *= time.Second

			pipesInfo = append(pipesInfo, info)
		}

		err := tmpl.Execute(w, pipesInfo)
		if err != nil {
			logger.Errorf("can't execute html template: %s", err.Error())
			_, _ = fmt.Fprintf(w, "<html><body>can't render html: %s</body></html>", err.Error())
			return
		}
	}
}
