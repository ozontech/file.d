package discard

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config     *Config
	controller pipeline.ActionController
}

type Config map[string]string

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "rename",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, controller pipeline.ActionController) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the rename plugin")
	}
	p.controller = controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) {
	for from, to := range *p.config {
		v := event.JSON.Get(from)
		if v == nil {
			continue
		}

		event.JSON.Set(to, v)
		event.JSON.Del(from)
	}

	p.controller.Propagate()
}
