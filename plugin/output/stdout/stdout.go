package stdout

import (
	"fmt"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It writes events to stdout(also known as console).
}*/

const outPluginType = "stdout"

type Plugin struct {
	controller pipeline.OutputPluginController
}

type Config struct{}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    outPluginType,
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
}

func (_ *Plugin) Stop() {}

func (p *Plugin) Out(event *pipeline.Event) {
	fmt.Println(event.Root.EncodeToString())
	p.controller.Commit(event)
}
