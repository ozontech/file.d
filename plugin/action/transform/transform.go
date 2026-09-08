package transform

import (
	"errors"
	"fmt"
	"sync"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"go.uber.org/zap"
)

var (
	programCacheMu sync.Mutex
	programCache   = make(map[string]Program)
)

type Program struct {
	expressions []core.Expr
}

/*{ introduction
It transforms events with programs written in a small expression language.
A single `transform` action can rename, reshape, parse and delete fields — work that
otherwise takes a chain of single-purpose actions.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: transform
      source: |
        # parse lines like "INFO 2025-05-25 11:11:11,222 [shard 1] compaction - done"
        m = capture(.log, r'^(?P<level>\S+)\s+(?P<time>\S+ \S+)\s+\[(?P<shard>[^\]]+)\]\s+(?P<operation>\S+)\s+-\s+(?P<message>.+)$')
        if m != null {
          .level = m.level
          .time = m.time
          .shard = m.shard
          .operation = m.operation
          .message = m.message
          del .log
        }
    ...
```

The event `{"log":"INFO 2025-05-25 11:11:11,222 [shard 1] compaction - done"}` becomes:
```json
{
  "level": "INFO",
  "time": "2025-05-25 11:11:11,222",
  "shard": "shard 1",
  "operation": "compaction",
  "message": "done"
}
```

The program is compiled once at pipeline start; an invalid program fails fast at startup.
A runtime error (e.g. a type error on a particular event) stops the program for that
event only: the error is logged and the event continues down the pipeline, keeping
the fields that were set before the error.
}*/

type Plugin struct {
	config           *Config
	program          Program
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The transformation program executed for every event.
	// > See the language reference and the list of built-in functions below.
	Source string `json:"source"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "transform",
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

	cacheKey := fmt.Sprintf("%s_%d", params.PipelineName, params.Index)

	programCacheMu.Lock()
	defer programCacheMu.Unlock()

	program, ok := programCache[cacheKey]
	if !ok {
		p.logger.Info("create transform compiler")
		cmp, err := compiler.NewCompiler(p.config.Source)
		if err != nil {
			p.logger.Fatal("failed to create compiler", zap.Error(err))
		}

		exprs, err := cmp.Compile()
		if err != nil {
			p.logger.Fatal("compilation error", zap.Error(err))
		}

		if err := compiler.ValidateCalls(exprs, stdlib.GetRegistry()); err != nil {
			p.logger.Fatal("validation error", zap.Error(err))
		}

		program = Program{exprs}
		programCache[cacheKey] = program
	}

	p.program = program
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	target := runtime.NewRootTarget(event.Root, event.SourceName, make(map[string]string))
	ctx := runtime.NewContext(target, stdlib.GetRegistry())

	for _, expr := range p.program.expressions {
		_, err := expr.Eval(ctx)
		if err != nil {
			if errors.Is(err, core.AbortError) {
				p.logger.Debug("transform program aborted")
				return pipeline.ActionPass
			}
			p.logger.Error("transform runtime error", zap.String("position", expr.Pos().String()), zap.Error(err))
			return pipeline.ActionPass
		}
	}

	return pipeline.ActionPass
}
