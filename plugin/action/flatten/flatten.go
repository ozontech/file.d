package flatten

import (
	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin extracts object keys and adds them into the root with some prefix. If provided field isn't object, event will be skipped.

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

//! config-params
//^ config-params
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Defines field that should be flattened.
	Field  cfg.FieldSelector `json:"field" required:"true" parse:"selector"` //*
	Field_ []string

	//> @3 @4 @5 @6
	//>
	//> Which prefix to use for extracted fields.
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
	node := event.Root.Dig(p.config.Field_...)

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
