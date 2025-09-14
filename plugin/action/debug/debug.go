package debug

import (
	"encoding/json"
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

}*/

type Plugin struct {
	logger       *zap.Logger
	config       *Config
	pipelineName string
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
	p.pipelineName = params.PipelineName

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
	loggerByPipeline   = make(map[string]*zap.Logger)
	loggerByPipelineMu sync.Mutex
)

func (p *Plugin) Stop() {
	loggerByPipelineMu.Lock()
	defer loggerByPipelineMu.Unlock()
	delete(loggerByPipeline, p.pipelineName)
}

// return shared logger between concurrent running processors
func (p *Plugin) setupLogger(parentLogger *zap.Logger, config *Config) {
	if config.Interval_ == 0 {
		p.logger = parentLogger
	}

	loggerByPipelineMu.Lock()
	defer loggerByPipelineMu.Unlock()

	lg, ok := loggerByPipeline[p.pipelineName]
	if !ok {
		// enable sampler
		lg = parentLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSamplerWithOptions(parentLogger.Core(), config.Interval_, config.First, config.Thereafter)
		}))
		loggerByPipeline[p.pipelineName] = lg
	}
	p.logger = lg
}
