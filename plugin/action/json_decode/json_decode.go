package json_decode

import (
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config *Config
}

type Config struct {
	Field string `json:"field"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "json_decode",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	if p.config.Field == "" {
		logger.Fatalf("no field provided for json decode plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Reset() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Fields.Dig(p.config.Field)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	node, err := event.SubparseJSON(jsonNode.AsBytes())
	if err != nil {
		return pipeline.ActionPass
	}

	if !node.IsObject() {
		return pipeline.ActionPass
	}

	jsonNode.Suicide()

	// place decoded object under root
	event.Fields.MergeWith(node)

	return pipeline.ActionPass
}
