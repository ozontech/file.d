package event_to_metrics

import (
	"maps"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/doif"
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

	Metrics []Metric
	mu      *sync.Mutex
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

	Metrics []Metric `json:"metrics" slice:"true" required:"true"` // *
}

type Metric struct {
	Name   string            `json:"name"`
	Type   string            `json:"type"`
	Value  string            `json:"value"`
	Labels map[string]string `json:"labels"`
	TTL    cfg.Duration      `json:"ttl" parse:"duration"` // *
	TTL_   time.Duration

	DoIfCheckerMap map[string]any `json:"do_if"`
	DoIfChecker    *doif.Checker

	use bool
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
	p.mu = &sync.Mutex{}
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.pluginController = params.Controller
	p.Metrics = prepareCheckersForMetrics(p.config.Metrics, p.logger)

	format, err := xtime.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.config.TimeFieldFormat
	}
	p.format = format
}

func prepareCheckersForMetrics(metrics []Metric, logger *zap.Logger) []Metric {
	for i := range metrics {
		m := &metrics[i]
		if m.DoIfCheckerMap != nil {
			var err error
			m.DoIfChecker, err = doif.NewFromMap(m.DoIfCheckerMap)
			if err != nil {
				logger.Fatal("can't init do_if for mask", zap.Error(err))
			}
		} else {
			m.use = true
		}
	}

	return metrics
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.mu.Lock()
	copyMetrics := make([]Metric, 0, len(p.Metrics))
	for i := range p.Metrics {
		if p.Metrics[i].DoIfChecker == nil {
			copyMetrics = append(copyMetrics, p.Metrics[i])
		} else {
			if !p.config.Metrics[i].DoIfChecker.Check(event.Root) {
				continue
			}
			copyMetrics = append(copyMetrics, p.Metrics[i])
		}
		if p.Metrics[i].Labels != nil {
			copyMetrics[len(copyMetrics)-1].Labels = make(map[string]string, len(p.Metrics[i].Labels))
			maps.Copy(copyMetrics[len(copyMetrics)-1].Labels, p.Metrics[i].Labels)
		}
	}
	p.mu.Unlock()

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

	children := make([]*insaneJSON.Node, 0, len(copyMetrics))
	for _, metric := range copyMetrics {
		elem := new(insaneJSON.Node)
		object := elem.MutateToObject()

		object.AddField("name").MutateToBytes([]byte(metric.Name))
		object.AddField("type").MutateToBytes([]byte(metric.Type))
		object.AddField("ttl").MutateToInt64(metric.TTL_.Milliseconds())
		object.AddField("timestamp").MutateToInt64(ts.UnixMilli())

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
		return pipeline.ActionDiscard
	}

	p.pluginController.Spawn(event, children)
	return pipeline.ActionBreak
}
