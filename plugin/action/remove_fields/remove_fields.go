package remove_fields

import (
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin removes list of the fields of the event and keeps others.
}*/
type Plugin struct {
	config    *Config
	fieldsBuf []string
}

//! config-params
//^ config-params
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> List of the fields to remove.
	Fields []string `json:"fields"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "remove_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
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
