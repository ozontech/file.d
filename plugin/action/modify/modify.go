package modify

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config *Config
}

type Config map[string]string

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "modify",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the rename plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Reset() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for field, value := range *p.config {
		if value == "" {
			continue
		}

		if value[0] == '$' {
			value = event.Root.Dig(value[1:]).AsString()
			if value == "" {
				continue
			}
		}

		event.Root.AddFieldNoAlloc(event.Root, field).MutateToString(value)
	}

	return pipeline.ActionPass
}
