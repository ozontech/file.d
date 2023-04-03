package add_file_name

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It adds a field containing the file name to the event.
It is only applicable for input plugins k8s and file.
}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to which put the file name. Must be a string.
	// >
	// > Warn: it overrides fields if it contains non-object type on the path. For example:
	// > if `field` is `info.level` and input
	// > `{ "info": [{"userId":"12345"}] }`,
	// > output will be: `{ "info": {"level": <level>} }`
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"file_name"` // *
	Field_ []string
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "add_file_name",
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
	pipeline.CreateNestedField(event.Root, p.config.Field_).MutateToString(event.SourceName)
	return pipeline.ActionPass
}
