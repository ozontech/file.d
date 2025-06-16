package event_to_metrics

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
Get metric from event
}*/

type Plugin struct {
	config           *Config
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
}

// ! config-params
// ^ config-params
type Config struct {
	Metrics []Metric
}

type Metric struct {
	Name   string            `json:"name"`
	Value  string            `json:"value"`
	Labels map[string]string `json:"labels"`
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "event_to_metrics",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.pluginController = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	children := make([]*insaneJSON.Node, 0, len(p.config.Metrics))
	for _, metric := range p.config.Metrics {
		elem := new(insaneJSON.Node)
		object := elem.MutateToObject()

		object.AddField("name").MutateToBytes([]byte(metric.Name))
		if len(metric.Value) == 0 {
			object.AddField("value").MutateToInt(1)
		} else {
			valueNode := event.Root.Dig(metric.Value).AsFloat()
			object.AddField("value").MutateToFloat(valueNode)
		}

		if len(metric.Labels) > 0 {
			labelsObject := object.AddField("labels").MutateToObject()

			for labelName, labelValue := range metric.Labels {
				node := event.Root.Dig(labelValue)
				value := node.AsString()
				labelsObject.AddField(labelName).MutateToBytes([]byte(value))
			}
		}

		children = append(children, elem)
	}

	if len(children) == 0 {
		// zero array or an array that does not contain objects
		return pipeline.ActionPass
	}

	p.pluginController.Spawn(event, children)
	return pipeline.ActionBreak
}
