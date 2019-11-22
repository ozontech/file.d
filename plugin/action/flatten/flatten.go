package flatten

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
		Type:    "flatten",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	if p.config.Field == "" {
		logger.Fatalf("no field provided for flatten plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field)

	if !node.IsObject() {
		return pipeline.ActionPass
	}

	node.Suicide()

	fields := node.AsFields()
	for _, field := range fields {
		l := len(event.Buf)
		event.Buf = append(event.Buf, p.config.Prefix...)
		event.Buf = append(event.Buf, field.AsString()...)
		field.MutateToField(pipeline.ByteToStringUnsafe(event.Buf[l:]))
	}

	// place decoded object under root
	event.Root.MergeWith(node)

	return pipeline.ActionPass
}
