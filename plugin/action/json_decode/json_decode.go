package json_decode

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.
}*/
type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to decode. Must be a string.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > A prefix to add to decoded object keys.
	Prefix string `json:"prefix" default:""` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "json_decode",
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
	jsonNode := event.Root.Dig(p.config.Field_...)
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
