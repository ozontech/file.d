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
	Field  string `json:"field"`
	Prefix string `json:"prefix"`
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

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field)
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

	if p.config.Prefix != "" {
		fields := node.AsFields()
		for _, field := range fields {
			l := len(event.Buf)
			event.Buf = append(event.Buf, p.config.Prefix...)
			event.Buf = append(event.Buf, field.AsString()...)
			field.MutateToField(pipeline.ByteToStringUnsafe(event.Buf[l:]))
		}
	}

	// place decoded object under root
	event.Root.MergeWith(node)

	return pipeline.ActionPass
}
