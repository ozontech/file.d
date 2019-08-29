package fake

import (
	"sync"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type FakePlugin struct {
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
	return &FakePlugin{}, &Config{}
}

func (p *FakePlugin) Start(config pipeline.AnyConfig, head pipeline.Head, doneWg *sync.WaitGroup) {
	p.head = head
}

func (p *FakePlugin) Stop() {
}

func (p *FakePlugin) Commit(event *pipeline.Event) {
	if p.acceptFn != nil {
		p.acceptFn(event)
	}

	p.done.Done()
}

func (p *FakePlugin) Wait() {
	p.done.Wait()
}

func (p *FakePlugin) SetAcceptFn(fn func(event *pipeline.Event)) {
	p.acceptFn = fn
}

func (p *FakePlugin) Accept(sourceId pipeline.SourceId, additional string, from int64, delta int64, bytes []byte) {
	p.done.Add(1)
	p.head.Accept(sourceId, additional, from, delta, bytes)
}
