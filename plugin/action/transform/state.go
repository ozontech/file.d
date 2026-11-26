package transform

import (
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
)

// executionState belongs to one active Do call. Its target and context may be
// reused only after all references to the previous event have been cleared.
type executionState struct {
	ctx    *runtime.Context
	target *runtime.RootTarget
}

func newExecutionState() *executionState {
	target := runtime.NewRootTarget(nil, "", nil)
	return &executionState{
		ctx:    runtime.NewContext(target, stdlib.GetRegistry()),
		target: target,
	}
}

func (p *Plugin) acquireState(event *pipeline.Event) *executionState {
	// Pipeline processors normally call their own plugin sequentially. Checkout
	// also preserves isolation for concurrent or reentrant calls of one plugin.
	state := p.state.Swap(nil)
	if state == nil {
		state = newExecutionState()
	}
	state.target.Root = event.Root
	state.target.SourceName = event.SourceName
	return state
}

func (p *Plugin) releaseState(state *executionState) {
	state.ctx.Reset()
	state.target.Reset()
	// Retain at most one idle state, even after overlapping executions.
	p.state.CompareAndSwap(nil, state)
}
