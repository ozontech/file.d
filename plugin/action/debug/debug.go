package debug

import (
	"encoding/json"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It logs event to stderr. Useful for debugging.

It may sample by logging the `first` N entries each tick.
If more events are seen during the same `interval`,
every `thereafter` message is logged and the rest are dropped.

For example,

```yaml
- type: debug
  interval: 1s
  first: 10
  thereafter: 5
```

This will log the first 10 events in a one second interval as-is.
Following that, it will allow through every 5th event in that interval.

}*/

type Plugin struct {
	logger *zap.Logger
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	Interval  cfg.Duration `json:"interval" parse:"duration"` // *
	Interval_ time.Duration
	// > @3@4@5@6
	First int `json:"first"` // *
	// > @3@4@5@6
	// >
	// > Check the example above for more information.
	Thereafter int `json:"thereafter"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "debug",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(anyConfig pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	config := anyConfig.(*Config)

	parentLogger := params.Logger.Desugar()

	parentLogger.Sugar().Errorf("%v %v %v", config.Interval_, config.First, config.Thereafter)
	if config.Interval_ != 0 {
		// enable sampler
		p.logger = parentLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(parentLogger.Core(), config.Interval_, config.First, config.Thereafter)
		}))
	} else {
		p.logger = parentLogger
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if ce := p.logger.Check(zapcore.InfoLevel, "new event"); ce != nil {
		ce.Write(
			zap.Int64("offset", event.Offset),
			zap.Any("event", json.RawMessage(event.Root.EncodeToString())),
		)
	}
	return pipeline.ActionPass
}
