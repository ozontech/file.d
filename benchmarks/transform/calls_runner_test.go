package transformbench

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/doif"
	"github.com/ozontech/file.d/plugin/action/modify"
	"github.com/ozontech/file.d/plugin/action/rename"
	"github.com/ozontech/file.d/plugin/action/transform"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const transformCallsSourceName = "transform_calls"

type transformCallScenario struct {
	name   string
	source string
	inputs []transformCallCase
	// No actions means this workload is transform-only. Otherwise these are
	// the comparator's actions, in execution order.
	actions []transformCallAction
	// Additional correctness cases are not part of the measured input corpus.
	cases []transformCallCase
}

type transformCallCase struct {
	name  string
	input string
	want  string
}

type transformCallAction struct {
	config pipeline.AnyConfig
	doIf   map[string]any
}

type transformCallVariant struct {
	name    string
	actions []transformCallAction
}

func (s *transformCallScenario) variants(tb testing.TB) []transformCallVariant {
	tb.Helper()
	require.NotEmpty(tb, s.inputs, "scenario %s has no measured inputs", s.name)
	variants := []transformCallVariant{{
		name:    "transform",
		actions: []transformCallAction{{config: &transform.Config{Source: s.source}}},
	}}
	if len(s.actions) == 0 {
		return variants
	}

	name := "modify"
	for _, action := range s.actions {
		switch action.config.(type) {
		case *modify.Config:
		case *rename.Config:
			require.Len(tb, s.actions, 1, "rename comparison must use one action")
			name = "rename"
		default:
			tb.Fatalf("unsupported comparator config %T", action.config)
		}
	}
	if len(s.actions) > 1 {
		name = fmt.Sprintf("modify_x%d", len(s.actions))
	}
	return append(variants, transformCallVariant{name: name, actions: s.actions})
}

type transformCallStep struct {
	run  func(*pipeline.Event) pipeline.ActionResult
	doIf *doif.Checker
}

func (v *transformCallVariant) runner(tb testing.TB, logger *zap.Logger) func(*pipeline.Event) pipeline.ActionResult {
	tb.Helper()
	steps := make([]transformCallStep, 0, len(v.actions))
	for i, action := range v.actions {
		require.NotNil(tb, action.config)
		var plugin pipeline.ActionPlugin
		var step transformCallStep
		switch action.config.(type) {
		case *transform.Config:
			p := &transform.Plugin{}
			plugin, step.run = p, p.Do
		case *modify.Config:
			p := &modify.Plugin{}
			plugin, step.run = p, p.Do
		case *rename.Config:
			p := &rename.Plugin{}
			plugin, step.run = p, p.Do
		default:
			tb.Fatalf("unsupported action config %T", action.config)
		}
		if action.doIf != nil {
			var err error
			step.doIf, err = doif.NewFromMap(action.doIf)
			require.NoError(tb, err)
		}
		plugin.Start(action.config, &pipeline.ActionPluginParams{
			Logger: logger.Sugar(),
			Index:  i,
			PluginDefaultParams: pipeline.PluginDefaultParams{
				PipelineName: "benchmark_" + tb.Name(),
			},
		})
		tb.Cleanup(plugin.Stop)
		steps = append(steps, step)
	}
	// Keep the original direct Do call for unconditional single plugins.
	if len(steps) == 1 && steps[0].doIf == nil {
		return steps[0].run
	}
	return func(event *pipeline.Event) pipeline.ActionResult {
		return runTransformCallSteps(event, steps)
	}
}

func runTransformCallSteps(event *pipeline.Event, steps []transformCallStep) pipeline.ActionResult {
	for _, step := range steps {
		// Like processor.isMatch, evaluate conditions against the current event,
		// including changes made by preceding actions.
		if step.doIf != nil && !step.doIf.Check(doif.NewEventData(event.Root)) {
			continue
		}
		if result := step.run(event); result != pipeline.ActionPass {
			return result
		}
	}
	return pipeline.ActionPass
}

type transformCallEvent struct {
	event      pipeline.Event
	result     pipeline.ActionResult
	inputIndex int
}

func newTransformCallEvents(tb testing.TB, count int) []transformCallEvent {
	tb.Helper()
	events := make([]transformCallEvent, count)
	for i := range events {
		events[i].event = pipeline.Event{Root: insaneJSON.Spawn(), SourceName: transformCallsSourceName}
		// Spawn can retain decoder memory from a different workload. Start
		// each variant with fresh storage, then warm and reuse it normally.
		// This setup is outside all measured metrics.
		events[i].event.Root.ReleaseMem()
	}
	tb.Cleanup(func() {
		for i := range events {
			insaneJSON.Release(events[i].event.Root)
		}
	})
	return events
}

// resetTransformCallEvents advances through a cyclic corpus. The caller keeps
// next across batches and resets it to zero after warmup, before measurement.
func resetTransformCallEvents(tb testing.TB, events []transformCallEvent, inputs []transformCallCase, next int) int {
	tb.Helper()
	for i := range events {
		if err := events[i].event.Root.DecodeString(inputs[next].input); err != nil {
			tb.Fatal(err)
		}
		events[i].inputIndex = next
		next = (next + 1) % len(inputs)
	}
	return next
}

func runTransformCallEvents(events []transformCallEvent, run func(*pipeline.Event) pipeline.ActionResult) {
	for i := range events {
		events[i].result = run(&events[i].event)
	}
}

func decodeTransformCallFields(tb testing.TB, input string) map[string]any {
	tb.Helper()
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	var fields map[string]any
	require.NoError(tb, decoder.Decode(&fields))
	return fields
}

func (c *transformCallCase) checkResult(tb testing.TB, event *transformCallEvent) {
	tb.Helper()
	require.Equal(tb, pipeline.ActionPass, event.result)
	// Compare the entire event, including types and untouched fields. All
	// serialization and assertions stay outside the measured interval.
	require.Equal(tb, decodeTransformCallFields(tb, c.want),
		decodeTransformCallFields(tb, event.event.Root.EncodeToString()), "input %s", c.name)
}
