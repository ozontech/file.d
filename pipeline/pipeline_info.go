package pipeline

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/ozontech/file.d/logger"
	"go.uber.org/atomic"
)

//go:embed template
var PipelineTpl embed.FS

const (
	InfoPipelineHTML = "template/html/pipeline_info.html"
	StartPageHTML    = "template/html/start_page.html"
)

type pluginsObservabilityInfo struct {
	In            inObservabilityInfo       `json:"in"`
	Out           outObservabilityInfo      `json:"out"`
	ActionPlugins []actionObservabilityInfo `json:"actions"`
	Changes       ChangesDTO                `json:"changes"`
}

type inObservabilityInfo struct {
	PluginName string `json:"plugin_name"`
	InfoMap    map[string]any
}

type outObservabilityInfo struct {
	PluginName     string         `json:"plugin_name"`
	BatcherMinWait BatcherTimeDTO `json:"batcher_min_wait"`
	BatcherMaxWait BatcherTimeDTO `json:"batcher_max_wait"`
}

type batcherCounter struct {
	Seconds          int64 `json:"seconds"`
	BatchesCommitted int64 `json:"batches_committed"`
}

type actionObservabilityInfo struct {
	PluginName string              `json:"plugin_name"`
	MetricName string              `json:"metric_name"`
	Tracked    bool                `json:"tracked"`
	Statuses   []actionEventStatus `json:"statuses"`
}

type actionEventStatus struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
	Color string `json:"color"`
}

func (p *Pipeline) GetDeltas() *deltas {
	inputEvents := newDeltaWrapper()
	inputSize := newDeltaWrapper()
	outputEvents := newDeltaWrapper()
	outputSize := newDeltaWrapper()
	readOps := newDeltaWrapper()

	return p.incMetrics(inputEvents, inputSize, outputEvents, outputSize, readOps)
}

func (p *Pipeline) boardInfo(
	inputInfo *InputPluginInfo,
	actionInfos []*ActionPluginStaticInfo,
	outputInfo *OutputPluginInfo,
) (pluginsObservabilityInfo, error) {
	result := pluginsObservabilityInfo{}
	changes := p.GetChanges(p.GetDeltas())
	changes.Interval *= time.Second

	result.Changes = changes
	inInfo, err := p.input.GetObservabilityInfo()
	if err != nil {
		return pluginsObservabilityInfo{}, err
	}

	result.In = inObservabilityInfo{
		PluginName: inputInfo.Type,
		InfoMap:    inInfo,
	}

	obsInfo := p.output.GetObservabilityInfo()

	out := outObservabilityInfo{
		PluginName: outputInfo.Type,
	}
	out.BatcherMinWait = obsInfo.BatcherInformation.MinWait
	out.BatcherMaxWait = obsInfo.BatcherInformation.MaxWait

	result.Out = out

	for _, info := range actionInfos {
		action := actionObservabilityInfo{}
		action.PluginName = info.Type
		if info.MetricName == "" {
			result.ActionPlugins = append(result.ActionPlugins, action)
			continue
		}

		action.Tracked = true
		action.MetricName = info.MetricName

		var actionMetric *metrics
		for _, m := range p.metricsHolder.metrics {
			if m.name == info.MetricName {
				actionMetric = m

				for _, status := range []eventStatus{
					eventStatusReceived,
					eventStatusDiscarded,
					eventStatusPassed,
					eventStatusNotMatched,
					eventStatusCollapse,
					eventStatusHold,
				} {
					c := actionMetric.current.totalCounter[string(status)]
					if c == nil {
						c = atomic.NewUint64(0)
					}
					color := "#0d8bf0"
					switch status {
					case eventStatusNotMatched:
						color = "#8bc34a"
					case eventStatusPassed:
						color = "green"
					case eventStatusDiscarded:
						color = "red"
					case eventStatusCollapse:
						color = "#009688"
					case eventStatusHold:
						color = "#f050f0"
					}
					eventStatus := actionEventStatus{Name: string(status), Count: int64(c.Load()), Color: color}
					action.Statuses = append(action.Statuses, eventStatus)
				}
			}
		}
		result.ActionPlugins = append(result.ActionPlugins, action)
	}

	return result, nil
}

func (p *Pipeline) serveBoardInfoJSON(inputInfo *InputPluginInfo,
	actionInfos []*ActionPluginStaticInfo,
	outputInfo *OutputPluginInfo) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		result, err := p.boardInfo(inputInfo, actionInfos, outputInfo)
		if err != nil {
			_, _ = fmt.Fprintf(w, "can't get board info: %s", err.Error())
		}
		bytes, err := json.Marshal(result)
		if err != nil {
			_, _ = fmt.Fprintf(w, "can't get json info: %s", err.Error())
		}

		_, _ = w.Write(bytes)
	}
}

func (p *Pipeline) serveBoardInfo(
	inputInfo *InputPluginInfo,
	actionInfos []*ActionPluginStaticInfo,
	outputInfo *OutputPluginInfo) func(http.ResponseWriter, *http.Request) {
	tmpl, parseFSErr := template.ParseFS(PipelineTpl, InfoPipelineHTML)
	if parseFSErr != nil {
		logger.Errorf("can't parse html template: %s", parseFSErr.Error())
	}

	return func(w http.ResponseWriter, _ *http.Request) {
		if parseFSErr != nil {
			_, _ = fmt.Fprintf(w, "<html><body>can't parse html: %s</body></html>", parseFSErr.Error())
			return
		}

		boardInfo, err := p.boardInfo(inputInfo, actionInfos, outputInfo)
		if err != nil {
			_, _ = fmt.Fprintf(w, "can't get board info: %s", err.Error())
		}
		err = tmpl.Execute(w, boardInfo)
		if err != nil {
			logger.Errorf("can't execute html template: %s", err.Error())
			_, _ = fmt.Fprintf(w, "<html><body>can't render html: %s</body></html>", err.Error())
			return
		}
	}
}
