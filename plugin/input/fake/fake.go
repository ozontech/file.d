package fake

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It provides an API to test pipelines and other plugins.
}*/

type Plugin struct {
	controller pipeline.InputPluginController
	commitFn   func(event *pipeline.Event)
	inFn       func()
}

type Config struct{}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
		Type:    "fake",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.commitFn != nil {
		p.commitFn(event)
	}
}

// ! fn-list
// ^ fn-list

// > It sends a test event into the pipeline.
func (p *Plugin) In(sourceID pipeline.SourceID, sourceName string, offset pipeline.Offsets, bytes []byte) { // *
	if p.inFn != nil {
		p.inFn()
	}
	_ = p.controller.In(sourceID, sourceName, offset, bytes, false, nil)
}

// > It sets up a hook to make sure the test event has been successfully committed.
func (p *Plugin) SetCommitFn(fn func(event *pipeline.Event)) { // *
	p.commitFn = fn
}

// > It sets up a hook to make sure the test event has been passed to the plugin.
func (p *Plugin) SetInFn(fn func()) { // *
	p.inFn = fn
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}
