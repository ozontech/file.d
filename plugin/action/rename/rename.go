package discard

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config     *Config
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

func (p *Plugin) Start(config pipeline.AnyConfig) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the rename plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for from, to := range *p.config {
		v := event.JSON.Get(from)
		if v == nil {
			continue
		}

		event.JSON.Set(to, v)
		event.JSON.Del(from)
	}

	return pipeline.ActionPass
}
