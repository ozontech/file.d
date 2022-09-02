package add_host

import (
	"os"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin"
)

/*{ introduction
It adds field containing hostname to an event.
}*/

type Plugin struct {
	config *Config
	plugin.EmptyMetricRegister
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to which put the hostname. Must be a string.
	Field string `json:"field" default:"host" required:"true"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "add_host",
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
	hostname, _ := os.Hostname()

	event.Root.AddFieldNoAlloc(event.Root, p.config.Field).MutateToString(hostname)

	return pipeline.ActionPass
}
