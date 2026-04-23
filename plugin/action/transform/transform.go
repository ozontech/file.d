package transform

import (
	"log"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
}*/

type Plugin struct {
	config           *Config
	registry         *Registry
	program          *Program
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Transform plugin source code.
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

	p.registry = NewRegistry()
	p.registry.MustRegister(upcase{})

	prog, err := Compile(p.config.Source, p.registry)
	if err != nil {
		log.Fatal(err)
	}
	p.program = prog
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	target := NewRootTarget(event.Root, event.SourceName, nil)

	result, err := p.program.Run(target)
	if err != nil {
		p.logger.Error("transform runtime error: %v", zap.Error(err))
	}

	if result.Aborted {
		p.logger.Info("transform program aborted")
	}

	return pipeline.ActionPass
}
