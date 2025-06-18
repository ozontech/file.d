package event_to_metrics

import (
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtime"
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
	format           string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which defines the time when event was fired.
	// > It is used to detect the event throughput in a particular time range.
	// > If not set, the current time will be taken.
	TimeField  cfg.FieldSelector `json:"time_field" default:"time" parse:"selector"` // *
	TimeField_ []string

	// > @3@4@5@6
	// >
	// > It defines how to parse the time field format. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
	// > List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano"` // *

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

	format, err := xtime.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.config.TimeFieldFormat
	}
	p.format = format
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	var ts time.Time

	if len(p.config.TimeField_) != 0 {
		tsValue := event.Root.Dig(p.config.TimeField_...).AsString()
		t, err := xtime.ParseTime(p.format, tsValue)
		if err != nil || t.IsZero() {
			p.logger.Warn(
				"can't parse field with timestamp using format",
				zap.Any("time_field", p.config.TimeField),
				zap.String("TimeFieldFormat", p.config.TimeFieldFormat),
				zap.String("value", tsValue),
			)
			ts = time.Now()
		} else {
			ts = t
		}
	} else {
		ts = time.Now()
	}

	children := make([]*insaneJSON.Node, 0, len(p.config.Metrics))
	for _, metric := range p.config.Metrics {
		elem := new(insaneJSON.Node)
		object := elem.MutateToObject()

		object.AddField("name").MutateToBytes([]byte(metric.Name))
		object.AddField("timestamp").MutateToInt64(ts.UnixNano())

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
