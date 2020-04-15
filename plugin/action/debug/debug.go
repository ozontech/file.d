package debug

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
It logs event to stdout. Useful for debugging.
}*/
type Plugin struct {
	paths          [][]string
	names          []string
	preserveFields bool
}

type Config map[string]interface{}

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
