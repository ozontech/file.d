package metric

import (
	"strings"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
Metric plugin.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: metric
	  name: errors_total
	  labels:
	  	- level

    ...
```

}*/

type Plugin struct {
	config *Config

	logger *zap.Logger

	//  plugin metrics
	appliedMetric *prometheus.CounterVec
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The metric name.
	Name string `json:"name" default:"total"` // *

	// > @3@4@5@6
	// >
	// > Lists the event fields to add to the metric. Blank list means no labels.
	// > Important note: labels metrics are not currently being cleared.
	Labels []string `json:"labels"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "metric",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) makeMetric(ctl *metric.Ctl, name, help string, labels ...string) *prometheus.CounterVec {
	if name == "" {
		return nil
	}

	uniq := make(map[string]struct{})
	labelNames := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			p.logger.Fatal("empty label name")
		}
		if _, ok := uniq[label]; ok {
			p.logger.Fatal("metric labels must be unique")
		}
		uniq[label] = struct{}{}

		labelNames = append(labelNames, label)
	}

	return ctl.RegisterCounterVec(name, help, labelNames...)
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config) // copy shared config
	p.logger = params.Logger.Desugar()
	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.appliedMetric = p.makeMetric(ctl,
		p.config.Name,
		"Number of events",
		p.config.Labels...,
	)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if p.config.Name != "" {
		labelValues := make([]string, 0, len(p.config.Labels))
		for _, labelValuePath := range p.config.Labels {
			value := "not_set"
			if node := event.Root.Dig(labelValuePath); node != nil {
				value = strings.Clone(node.AsString())
			}

			labelValues = append(labelValues, value)
		}

		p.appliedMetric.WithLabelValues(labelValues...).Inc()
	}

	return pipeline.ActionPass
}
