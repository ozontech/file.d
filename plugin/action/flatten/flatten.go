package flatten

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It extracts the object keys and adds them into the root with some prefix. If the provided field isn't an object, an event will be skipped.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```
It transforms `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.
}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Defines the field that should be flattened.
	Field  cfg.FieldSelector `json:"field" required:"true" parse:"selector"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Which prefix to use for extracted fields.
	Prefix string `json:"prefix" default:""` // *
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
	pipeline.MergeToRoot(event.Root, node)

	return pipeline.ActionPass
}
