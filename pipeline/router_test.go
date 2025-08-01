package pipeline_test

import (
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
)

type FakeOutputPluginController struct {
}

func (f FakeOutputPluginController) Commit(event *pipeline.Event) {

}
func (f FakeOutputPluginController) Error(err string) {

}

func TestRouter(t *testing.T) {
	r := pipeline.NewRouter()

	outputCount := 0
	deadQueueOutputCount := 0

	plugin, config := devnull.Factory()
	outputPlugin := plugin.(*devnull.Plugin)
	outputPlugin.SetOutFn(func(event *pipeline.Event) {
		outputCount++
	})

	outputInfo := &pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	}

	r.SetOutput(outputInfo)

	deadQueuePlugin, deadQueueConfig := devnull.Factory()
	deadQueueOutputPlugin := deadQueuePlugin.(*devnull.Plugin)
	deadQueueOutputPlugin.SetOutFn(func(event *pipeline.Event) {
		deadQueueOutputCount++
	})

	deadQueueOutputInfo := &pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: deadQueueConfig,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: deadQueueOutputPlugin,
		},
	}
	r.SetDeadQueueOutput(deadQueueOutputInfo)
	params := test.NewEmptyOutputPluginParams()
	params.Router = *r
	params.Controller = FakeOutputPluginController{}
	r.Start(params)

	r.Out(newEvent(t))
	assert.Equal(t, 1, outputCount)
	assert.Equal(t, 0, deadQueueOutputCount)
}

func TestRouterDeadQueue(t *testing.T) {
	r := pipeline.NewRouter()

	outputCount := 0
	deadQueueOutputCount := 0

	plugin, config := devnull.Factory()
	outputPlugin := plugin.(*devnull.Plugin)
	outputPlugin.SetOutFn(func(event *pipeline.Event) {
		r.Fail(event)
	})

	outputInfo := &pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	}

	r.SetOutput(outputInfo)

	deadQueuePlugin, deadQueueConfig := devnull.Factory()
	deadQueueOutputPlugin := deadQueuePlugin.(*devnull.Plugin)
	deadQueueOutputPlugin.SetOutFn(func(event *pipeline.Event) {
		deadQueueOutputCount++
	})

	deadQueueOutputInfo := &pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: deadQueueConfig,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: deadQueueOutputPlugin,
		},
	}
	r.SetDeadQueueOutput(deadQueueOutputInfo)

	params := test.NewEmptyOutputPluginParams()
	params.Router = *r
	params.Controller = FakeOutputPluginController{}
	r.Start(params)

	r.Out(newEvent(t))
	assert.Equal(t, 0, outputCount)
	assert.Equal(t, 1, deadQueueOutputCount)
}

func newEvent(t *testing.T) *pipeline.Event {
	root, err := insaneJSON.DecodeString(`{}`)
	if err != nil {
		t.Skip() // ignore invalid input
	}
	return &pipeline.Event{
		Root: root,
		Buf:  make([]byte, 0, 1024),
	}
}
