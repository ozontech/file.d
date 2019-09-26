package keep_fields

import (
	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	config     *Config
	fieldsBuf  [][]byte
}

type Config struct {
	Fields []string `json:"fields"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "keep_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the keep fields plugin")
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Reset() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	fields := p.config.Fields
	p.fieldsBuf = p.fieldsBuf[:0]

	o, err := event.JSON.Object()
	if err != nil {
		logger.Panicf("why event isn't object?")
		panic("")
	}

	o.Visit(func(key []byte, v *fastjson.Value) {
		isInList := false
		for _, field := range fields {
			if field == pipeline.ByteToString(key) {
				isInList = true
				break
			}
		}
		if !isInList {
			p.fieldsBuf = append(p.fieldsBuf, key)
		}
	})

	for _, field := range p.fieldsBuf {
		event.JSON.Del(pipeline.ByteToString(field))
	}

	return pipeline.ActionPass
}
