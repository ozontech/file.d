package devnull

import (
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	"go.uber.org/atomic"
)

/*{ introduction
Provides API to test pipelines and other plugins.
}*/

type Plugin struct {
	controller    pipeline.OutputPluginController
	outFn         func(event *pipeline.Event)
	total         *atomic.Int64
}

type Config struct {
}

func init() {
	fd.DefaultPluginRegistry.RegisterOutput(&pipeline.PluginStaticInfo{
		Type:    "devnull",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.total = &atomic.Int64{}
}

//! fn-list
//^ fn-list

//> Sets up a hook to make sure test event have been successfully passed to output.
func (p *Plugin) SetOutFn(fn func(event *pipeline.Event)) { //*
	p.outFn = fn

}

func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	if p.outFn != nil {
		p.outFn(event)
	}

	p.controller.Commit(event)
}
