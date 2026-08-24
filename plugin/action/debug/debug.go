package debug

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const eventField = "event"

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

If it is needed to log every entry, logger without sampling can be used,

```yaml
- type: debug
  interval: 0s
```

}*/

type Plugin struct {
	logger         *zap.Logger
	config         *Config
	pipelineAction string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Tick interval for sampling logging. The first N entries with a given level and message
	// > each tick. If more Entries with the same level and message are seen during
	// > the same interval, every Mth message is logged and the rest are dropped.
	// >
	// > If set to 0, plugin uses parent logger without sampling.
	// >
	// > Check the example above for more information.
	Interval  cfg.Duration `json:"interval" parse:"duration"` // *
	Interval_ time.Duration
	// > @3@4@5@6
	// >
	// > Specifies the first N entries with a given level and message each tick.
	// >
	// > Check the example above for more information.
	First int `json:"first"` // *
	// > @3@4@5@6
	// >
	// > Specifies entries frequency after the first N entries.
	// > If greater than 0, every Mth message is logged and the rest are dropped.
	// > If set to 0, every entry after the first N are dropped.
	// >
	// > Check the example above for more information.
	Thereafter int `json:"thereafter"` // *

	// > @3@4@5@6
	// >
	// > 'message' field content.
	// > Use it to determine which 'debug' action has written the log.
	Message string `json:"message" default:"event sample"` // *
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
	p.config = anyConfig.(*Config)
	p.pipelineAction = fmt.Sprintf("%s_%d", params.PipelineName, params.Index)

	lg := params.Logger.Desugar()
	p.setupLogger(lg, p.config)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if ce := p.logger.Check(zapcore.InfoLevel, p.config.Message); ce != nil {
		ce.Write(
			zap.Int64("offset", event.Offset),
			zap.Any(eventField, json.RawMessage(event.Root.EncodeToString())),
		)
	}
	return pipeline.ActionPass
}

var (
	loggerByPipelineAction = make(map[string]*zap.Logger)
	loggerByPipelineMu     sync.Mutex
)

func (p *Plugin) Stop() {
	loggerByPipelineMu.Lock()
	defer loggerByPipelineMu.Unlock()
	delete(loggerByPipelineAction, p.pipelineAction)
}

// return shared logger between concurrent running processors
func (p *Plugin) setupLogger(parentLogger *zap.Logger, config *Config) {
	if config.Interval_ == 0 {
		p.logger = parentLogger
		return
	}

	loggerByPipelineMu.Lock()
	defer loggerByPipelineMu.Unlock()

	lg, ok := loggerByPipelineAction[p.pipelineAction]
	if !ok {
		// enable sampler
		lg = parentLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(parentLogger.Core(), config.Interval_, config.First, config.Thereafter)
		}))
		loggerByPipelineAction[p.pipelineAction] = lg
	}
	p.logger = lg
}
