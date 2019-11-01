package devnull

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type Plugin struct {
	controller pipeline.OutputPluginController
	outFn      func(event *pipeline.Event)
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginInfo{
		Type:    "devnull",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
}

func (p *Plugin) SetOutFn(fn func(event *pipeline.Event)) {
	p.outFn = fn

}
func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	if p.outFn != nil {
		p.outFn(event)
	}

	p.controller.Commit(event)
}
