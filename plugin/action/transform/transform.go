package transform

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/parser"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"go.uber.org/zap"
)

var (
	globalLexer   = parser.NewCompiledLexer()
	compilerCache = map[string]*compiler.Compiler{}
)

/*{ introduction
}*/

type Plugin struct {
	config           *Config
	registry         *core.Registry
	expressions      []core.Expr
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

	p.registry = core.NewRegistry()
	p.registry.MustRegister(stdlib.Upcase{})

	parser := parser.NewParser(globalLexer)
	tokens, err := parser.Parse(p.config.Source)

	if err != nil {
		p.logger.Fatal("parsing error", zap.Error(err))
	}

	cacheKey := fmt.Sprintf("%s_%d", params.PipelineName, params.Index)
	c, ok := compilerCache[cacheKey]
	if !ok {
		p.logger.Info("create compiler")
		c = compiler.NewCompiler(tokens)
		compilerCache[cacheKey] = c
	}

	exprs, err := c.Compile()
	if err != nil {
		p.logger.Fatal("compilation error", zap.Error(err))
	}

	if err := compiler.ValidateCalls(exprs, p.registry); err != nil {
		p.logger.Fatal("validation error", zap.Error(err))
	}

	p.expressions = exprs
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	target := runtime.NewRootTarget(event.Root, event.SourceName, nil)
	ctx := runtime.NewContext(target, p.registry)

	for _, expr := range p.expressions {
		fmt.Println(core.DumpAST(expr, 0))
		_, err := expr.Eval(ctx)
		if err != nil {
			if errors.Is(err, core.AbortError) {
				p.logger.Info("transform program aborted")
				return pipeline.ActionPass
			}
			p.logger.Error("transform runtime error", zap.String("position", expr.Pos().String()), zap.Error(err))
			return pipeline.ActionPass
		}
	}

	return pipeline.ActionPass
}
