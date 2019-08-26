package inputfake

import (
	"math/rand"
	"sync"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
}

type FakePlugin struct {
	heads    []*pipeline.Head
	acceptFn func(event *pipeline.Event)
	done     sync.WaitGroup
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "manual",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPluginPointer, pipeline.AnyConfigPointer) {
	return &FakePlugin{}, &Config{}
}

func (m *FakePlugin) Start(config pipeline.AnyConfigPointer, pipeline pipeline.Pipeline) {
	m.heads = pipeline.GetHeads()
}

func (m *FakePlugin) Stop() {
}

func (m *FakePlugin) Accept(event *pipeline.Event) {
	if m.acceptFn == nil {
		logger.Panicf("Accept function isn't set")
	}

	m.acceptFn(event)
	m.done.Done()
}

func (m *FakePlugin) Wait() {
	m.done.Wait()
}

func (m *FakePlugin) SetAcceptFn(fn func(event *pipeline.Event)) {
	m.acceptFn = fn
}

func (m *FakePlugin) Push(sourceId uint64, additional string, from int64, delta int64, bytes []byte) {
	m.done.Add(1)
	m.heads[rand.Int()%len(m.heads)].Push(m, sourceId, additional, from, delta, bytes)
}
