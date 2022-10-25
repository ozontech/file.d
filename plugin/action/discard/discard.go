package discard

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"time"
)

/*{ introduction
It drops an event. It is used in a combination with `match_fields`/`match_mode` parameters to filter out the events.

**An example for discarding informational and debug logs:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: discard
      match_fields:
        level: /info|debug/
    ...
```
}*/

const (
	tickTime = 1 * time.Second
	first    = 1
)

type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
	plugin.NoMetricsPlugin
}

type Config struct {
	IsLogging  bool `json:"is_logging" default:"false"`
	Thereafter int  `json:"thereafter" default:"100"`
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "discard",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	// new logger with sampler
	p.logger = zap.New(
		zapcore.NewSamplerWithOptions(params.Logger.Desugar().Core(), tickTime, first, p.config.Thereafter),
	).Named("fd").Named("action").Named("discard").Sugar()
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if p.config.IsLogging {
		p.logger.Infof("discarded event: %s", event.Root.EncodeToString())
	}
	return pipeline.ActionDiscard
}
