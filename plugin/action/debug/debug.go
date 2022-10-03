package debug

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It logs event to stdout. Useful for debugging.
}*/

type Plugin struct{}

type Config map[string]any

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "debug",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	buf, _ := event.Encode(nil)
	logger.Infof("%s", buf)

	return pipeline.ActionPass
}
