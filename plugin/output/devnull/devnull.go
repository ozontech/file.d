package devnull

import (
	"sync"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/atomic"
)

type Config struct {
}

type Plugin struct {
	controller    pipeline.OutputPluginController
	outFn         func(event *pipeline.Event)
	total         *atomic.Int64
	waitThreshold int64
	wg            *sync.WaitGroup
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.OutputPluginParams) {
	p.controller = params.Controller
	p.total = &atomic.Int64{}
	p.wg = &sync.WaitGroup{}
}

func (p *Plugin) SetOutFn(fn func(event *pipeline.Event)) {
	p.outFn = fn

}
func (p *Plugin) ResetWaitFor() {
	p.waitThreshold = int64(0)
}

func (p *Plugin) WaitFor(count int) {
	p.wg.Add(1)
	total := p.total.Load()
	p.waitThreshold = int64(count)
	if total > p.waitThreshold {
		panic("wrong output events")
	}
	if total == p.waitThreshold {
		return
	}
	p.wg.Wait()
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Out(event *pipeline.Event) {
	x := p.total.Add(1)
	if p.waitThreshold != 0 && x >= p.waitThreshold {
		y := p.total.Swap(0)
		if y > 0 {
			p.wg.Done()
		}
	}

	if p.outFn != nil {
		p.outFn(event)
	}

	p.controller.Commit(event)
}
