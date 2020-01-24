package fake

import (
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin provides methods to use in test scenarios:

@fns|signature-list
}*/
type Plugin struct {
	controller pipeline.InputPluginController
	commitFn   func(event *pipeline.Event)
	inFn       func()
}

type Config struct {
}

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

//! fns #4 /Plugin\)\s(.+)\s{/
//^ _ _ code

//> Sends test event into pipeline.
func (p *Plugin) In(sourceID pipeline.SourceID, sourceName string, offset int64, size int64, bytes []byte) { //*
	if p.inFn != nil {
		p.inFn()
	}
	p.controller.In(sourceID, sourceName, offset, bytes)
}

//> Sets up a hook to make sure test event have been successfully committed.
func (p *Plugin) SetCommitFn(fn func(event *pipeline.Event)) { //*
	p.commitFn = fn
}

//> Sets up a hook to make sure test event have been passed to plugin.
func (p *Plugin) SetInFn(fn func()) { //*
	p.inFn = fn
}
