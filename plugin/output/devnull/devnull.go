package devnull

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type DevNullPlugin struct {
	controller pipeline.OutputController
	dumpFn     func(event *pipeline.Event)
}

func init() {
	filed.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginInfo{
		Type:    "devnull",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &DevNullPlugin{}, &Config{}
}

func (p *DevNullPlugin) Start(config pipeline.AnyConfig, controller pipeline.OutputController) {
	p.controller = controller
}

func (p *DevNullPlugin) SetDumpFn(fn func(event *pipeline.Event)) {
	p.dumpFn = fn

}
func (p *DevNullPlugin) Stop() {
}

func (p *DevNullPlugin) Dump(event *pipeline.Event) {
	if p.dumpFn != nil {
		p.dumpFn(event)
	}

	p.controller.Commit(event)
}
