package keep_fields

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
It keeps the list of the event fields and removes others.
}*/
type Plugin struct {
	config    *Config
	fieldsBuf []string
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The list of the fields to keep.
	Fields []string `json:"fields"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "keep_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.fieldsBuf = p.fieldsBuf[:0]

	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for _, node := range event.Root.AsFields() {
		eventField := node.AsString()
		isInList := false
		for _, pluginField := range p.config.Fields {
			if pluginField == eventField {
				isInList = true
				break
			}
		}
		if !isInList {
			p.fieldsBuf = append(p.fieldsBuf, eventField)
		}
	}

	for _, field := range p.fieldsBuf {
		event.Root.Dig(field).Suicide()
	}

	return pipeline.ActionPass
}
