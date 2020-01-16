package fake

import (
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

type Config struct {
}

type Plugin struct {
	controller pipeline.InputPluginController
	commitFn   func(event *pipeline.Event)
	inFn       func()
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.controller = params.Controller
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.commitFn != nil {
		p.commitFn(event)
	}
}

func (p *Plugin) SetCommitFn(fn func(event *pipeline.Event)) {
	p.commitFn = fn
}

func (p *Plugin) SetInFn(fn func()) {
	p.inFn = fn
}

func (p *Plugin) In(sourceID pipeline.SourceID, sourceName string, offset int64, size int64, bytes []byte) {
	if p.inFn != nil {
		p.inFn()
	}
	p.controller.In(sourceID, sourceName, offset, bytes)
}
