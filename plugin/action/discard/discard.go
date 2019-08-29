package discard

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type DiscardPlugin struct {
	config     *Config
	controller pipeline.ActionController
}

type Config struct {
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "discard",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &DiscardPlugin{}, &Config{}
}

func (p *DiscardPlugin) Start(config pipeline.AnyConfig, controller pipeline.ActionController) {
	p.controller = controller
}

func (p *DiscardPlugin) Stop() {
}

func (p *DiscardPlugin) Do(event *pipeline.Event) {
	// just run fake commit :)
	p.controller.Commit(event)
}
