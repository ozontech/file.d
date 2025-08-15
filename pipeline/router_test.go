package pipeline_test

import (
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
)

type fakeOutputPluginController struct {
	mu      sync.Mutex
	commits []*pipeline.Event
	errors  []string
}

func (f *fakeOutputPluginController) Commit(event *pipeline.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, event)
}
func (f *fakeOutputPluginController) Error(err string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errors = append(f.errors, err)
}

func (f *fakeOutputPluginController) getCommits() []*pipeline.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.commits
}

func (f *fakeOutputPluginController) getErrors() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.errors
}

func TestRouterNormalProcessing(t *testing.T) {
	t.Parallel()

	r := pipeline.NewRouter()
	controller := &fakeOutputPluginController{}

	// Setup main output that succeeds
	var outputCount atomic.Int32
	outputPlugin, outputConfig := createDevNullPlugin(func(event *pipeline.Event) {
		outputCount.Add(1)
	})

	r.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Config: outputConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: outputPlugin},
	})

	// Setup dead queue that shouldn't be used
	var deadQueueCount atomic.Int32
	deadQueuePlugin, deadQueueConfig := createDevNullPlugin(func(event *pipeline.Event) {
		deadQueueCount.Add(1)
	})

	r.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Config: deadQueueConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: deadQueuePlugin},
	})

	params := test.NewEmptyOutputPluginParams()
	params.PipelineName = "test_pipeline"
	params.Router = r
	params.Controller = controller
	r.Start(params)
	defer r.Stop()

	// Send test event
	event := newEvent(t)
	r.Out(event)

	// Wait for processing
	assert.Eventually(t, func() bool {
		return outputCount.Load() == 1
	}, 100*time.Millisecond, 10*time.Millisecond, "event should be processed")

	assert.Equal(t, int32(0), deadQueueCount.Load(), "dead queue should not be used")
	assert.Len(t, controller.getCommits(), 1, "should commit successful event")
	assert.Empty(t, controller.getErrors(), "should not produce errors")
}

func TestRouterDeadQueueProcessing(t *testing.T) {
	t.Parallel()

	r := pipeline.NewRouter()
	controller := &fakeOutputPluginController{}

	// Setup main output that fails
	var outputCount atomic.Int32
	outputPlugin, outputConfig := createDevNullPlugin(func(event *pipeline.Event) {
		outputCount.Add(1)
		r.Fail(event)
	})

	r.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Config: outputConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: outputPlugin},
	})

	// Setup dead queue
	var deadQueueCount atomic.Int32
	deadQueuePlugin, deadQueueConfig := createDevNullPlugin(func(event *pipeline.Event) {
		deadQueueCount.Add(1)
	})

	r.SetDeadQueueOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  &pipeline.PluginStaticInfo{Config: deadQueueConfig},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{Plugin: deadQueuePlugin},
	})

	params := test.NewEmptyOutputPluginParams()
	params.PipelineName = "test_pipeline"
	params.Router = r
	params.Controller = controller
	r.Start(params)
	defer r.Stop()

	// Send test event
	event := newEvent(t)
	r.Out(event)

	// Wait for processing
	assert.Eventually(t, func() bool {
		return deadQueueCount.Load() == 1
	}, 100*time.Millisecond, 10*time.Millisecond, "event should go to dead queue")

	assert.Equal(t, int32(1), outputCount.Load(), "main output should try to process")
	assert.Len(t, controller.getCommits(), 2, "should commit successful event")
	assert.Empty(t, controller.getErrors(), "should not produce errors")
}

func createDevNullPlugin(outFn func(event *pipeline.Event)) (*devnull.Plugin, pipeline.AnyConfig) {
	plugin, config := devnull.Factory()
	p := plugin.(*devnull.Plugin)
	p.SetOutFn(outFn)
	return p, config
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
