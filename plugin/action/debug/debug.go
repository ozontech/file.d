package debug

import (
	"encoding/json"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

/*{ introduction
It logs event to stdout. Useful for debugging.
}*/

type Plugin struct {
	logger     *zap.Logger
	controller pipeline.ActionPluginController
}

type Config map[string]any

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "debug",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger.Desugar()
	p.controller = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	prevAction, ok := p.controller.ActionTypeByIndex(event.Action() - 1)
	if !ok {
		prevAction = "initial"
	}
	if ce := p.logger.Check(zapcore.InfoLevel, prevAction); ce != nil {
		ce.Write(
			zap.Int64("offset", event.Offset),
			zap.Any("event", json.RawMessage(event.Root.EncodeToString())),
		)
	}
	return pipeline.ActionPass
}
