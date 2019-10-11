package fake

import (
	"sync"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type Plugin struct {
	head     pipeline.Head
	acceptFn func(event *pipeline.Event)
	done     sync.WaitGroup
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "fake",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, head pipeline.Head, doneWg *sync.WaitGroup) {
	p.head = head
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
	if p.acceptFn != nil {
		p.acceptFn(event)
	}

	p.done.Done()
}

func (p *Plugin) Wait() {
	p.done.Wait()
}

func (p *Plugin) SetAcceptFn(fn func(event *pipeline.Event)) {
	p.acceptFn = fn
}

func (p *Plugin) In(sourceId pipeline.SourceID, sourceName string, offset int64, size int64, bytes []byte) {
	p.done.Add(1)
	p.head.In(sourceId, sourceName, offset, bytes)
}
