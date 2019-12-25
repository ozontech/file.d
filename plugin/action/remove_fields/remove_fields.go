package remove_fields

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config    *Config
	fieldsBuf []string
}

type Config struct {
	Fields []string `json:"fields"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "remove_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the remove fields plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.fieldsBuf = p.fieldsBuf[:0]

	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for _, field := range p.config.Fields {
		event.Root.Dig(field).Suicide()
	}

	return pipeline.ActionPass
}
