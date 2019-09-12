package json_decode

import (
	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config     *Config
	controller pipeline.ActionController
	parsers    []*fastjson.Parser
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

func (p *Plugin) Start(config pipeline.AnyConfig, controller pipeline.ActionController) {
	p.config = config.(*Config)
	p.controller = controller
	p.parsers = make([]*fastjson.Parser, 0, 0)

	if p.config.Field == "" {
		logger.Fatalf("no field provided for json decode plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) {
	defer p.controller.Propagate()

	json := event.JSON.GetStringBytes(p.config.Field)
	if json == nil {
		return
	}

	parsed, err := event.ParseJSON(json)
	if err != nil {
		return
	}

	o, err := parsed.Object()
	if err != nil {
		return
	}

	event.JSON.Del(p.config.Field)

	// place decoded object under root
	o.Visit(func(key []byte, v *fastjson.Value) {
		event.JSON.Set(pipeline.ByteToString(key), v)
	})
}
