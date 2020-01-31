package flatten

import (
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin extracts object keys and adds them into root with some prefix.

Example:
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```

Will transform `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.
}*/
type Plugin struct {
	config *Config
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> To be filled
	Field string `json:"field" required:"true"` //*

	//> @3 @4 @5 @6
	//>
	//> To be filled
	Prefix string `json:"prefix" default:""` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "flatten",
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
