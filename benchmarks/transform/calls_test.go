package transformbench

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/modify"
	"github.com/ozontech/file.d/plugin/action/rename"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestTransformBenchmarkScenarios(t *testing.T) {
	for _, scenario := range transformCallScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

// These small fixtures exercise the runner's supported comparison shapes.
// Measured workloads are added separately, in the next implementation stages.
func TestTransformBenchmarkRunners(t *testing.T) {
	type testCase struct {
		name     string
		scenario transformCallScenario
		variants []string
	}
	tests := []testCase{
		{
			name: "conditional_modify_chain",
			scenario: transformCallScenario{
				source: `
					if .source == "api" {
						.stage = "ready"
						.out = .stage
					}
				`,
				actions: []transformCallAction{
					{
						config: &modify.Config{"stage": "ready"},
						doIf:   map[string]any{"op": "equal", "field": "source", "values": "api"},
					},
					{
						config: &modify.Config{"out": "${stage}"},
						doIf:   map[string]any{"op": "equal", "field": "stage", "values": "ready"},
					},
				},
				inputs: []transformCallCase{
					{
						name:  "match",
						input: `{"source":"api","keep":7}`,
						want:  `{"source":"api","keep":7,"stage":"ready","out":"ready"}`,
					},
					{
						name:  "no_match_after_match",
						input: `{"source":"other","out":"keep"}`,
						want:  `{"source":"other","out":"keep"}`,
					},
					{
						name:  "match_again",
						input: `{"source":"api","out":"old"}`,
						want:  `{"source":"api","stage":"ready","out":"ready"}`,
					},
				},
			},
			variants: []string{"transform", "modify_x2"},
		},
		{
			name: "single_conditional_modify",
			scenario: transformCallScenario{
				source: `
					if .source == "api" {
						.out = "ready"
					}
				`,
				actions: []transformCallAction{
					{
						config: &modify.Config{"out": "ready"},
						doIf:   map[string]any{"op": "equal", "field": "source", "values": "api"},
					},
				},
				inputs: []transformCallCase{
					{
						name:  "match",
						input: `{"source":"api"}`,
						want:  `{"source":"api","out":"ready"}`,
					},
					{
						name:  "missing",
						input: `{}`,
						want:  `{}`,
					},
				},
			},
			variants: []string{"transform", "modify"},
		},
		{
			name: "rename_preserves_types",
			scenario: transformCallScenario{
				source: `
					.new = .old
					del .old
				`,
				actions: []transformCallAction{
					{
						config: &rename.Config{"old", "new"},
					},
				},
				inputs: []transformCallCase{
					{
						name:  "number",
						input: `{"old":42,"keep":true}`,
						want:  `{"new":42,"keep":true}`,
					},
					{
						name:  "string",
						input: `{"old":"value"}`,
						want:  `{"new":"value"}`,
					},
				},
			},
			variants: []string{"transform", "rename"},
		},
		{
			name: "transform_only",
			scenario: transformCallScenario{
				source: `.out = upcase(.source)`,
				inputs: []transformCallCase{
					{
						name:  "text",
						input: `{"source":"api"}`,
						want:  `{"source":"api","out":"API"}`,
					},
					{
						name:  "empty",
						input: `{"source":""}`,
						want:  `{"source":"","out":""}`,
					},
				},
			},
			variants: []string{"transform"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var names []string
			for _, variant := range tc.scenario.variants(t) {
				names = append(names, variant.name)
			}
			require.Equal(t, tc.variants, names)
			checkTransformCallScenario(t, &tc.scenario)
		})
	}
}

func checkTransformCallScenario(t *testing.T, scenario *transformCallScenario) {
	t.Helper()
	for _, variant := range scenario.variants(t) {
		t.Run(variant.name, func(t *testing.T) {
			logCore, logs := observer.New(zap.ErrorLevel)
			run := variant.runner(t, zap.New(logCore))
			events := newTransformCallEvents(t, 3)
			// Check measured and extra correctness inputs through the same runner,
			// reusing both the plugin and Root across different inputs.
			inputs := append(append([]transformCallCase{}, scenario.inputs...), scenario.cases...)
			next := 0
			completed := 0
			for batchIndex := 0; completed < 3*len(inputs); batchIndex++ {
				size := min(3-batchIndex%2, 3*len(inputs)-completed)
				batch := events[:size]
				next = resetTransformCallEvents(t, batch, inputs, next)
				for i := range batch {
					input := inputs[(completed+i)%len(inputs)]
					require.Equal(t, decodeTransformCallFields(t, input.input),
						decodeTransformCallFields(t, batch[i].event.Root.EncodeToString()))
				}
				runTransformCallEvents(batch, run)
				require.Empty(t, logs.All(), "unexpected plugin errors")
				for i := range batch {
					inputs[(completed+i)%len(inputs)].checkResult(t, &batch[i])
				}
				completed += len(batch)
			}
		})
	}
}

func TestTransformBenchmarkInputSequence(t *testing.T) {
	type testCase struct {
		name       string
		inputCount int
		batchSizes []int
	}
	tests := []testCase{
		{
			name:       "homogeneous",
			inputCount: 1,
			batchSizes: []int{3, 2, 3},
		},
		{
			name:       "partial_cycle",
			inputCount: 3,
			batchSizes: []int{2, 2, 1, 3},
		},
		{
			name:       "corpus_larger_than_batch",
			inputCount: 7,
			batchSizes: []int{3, 3, 1, 3, 3, 1, 3},
		},
		{
			name:       "production_batch_boundary",
			inputCount: 257,
			batchSizes: []int{256, 1, 256, 2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inputs := make([]transformCallCase, tc.inputCount)
			for i := range inputs {
				inputs[i].input = fmt.Sprintf(`{"id":%d}`, i)
			}
			events := newTransformCallEvents(t, transformCallsBatchSize)
			next, completed := 0, 0
			for _, size := range tc.batchSizes {
				batch := events[:size]
				next = resetTransformCallEvents(t, batch, inputs, next)
				for i := range batch {
					want := (completed + i) % tc.inputCount
					require.Equal(t, want, batch[i].inputIndex)
					require.Equal(t, int64(want), batch[i].event.Root.Dig("id").AsInt64())
					require.Nil(t, batch[i].event.Root.Dig("changed"))
					batch[i].event.Root.AddField("changed").MutateToBool(true)
				}
				completed += size
				require.Equal(t, completed%tc.inputCount, next)
			}
		})
	}
}

func TestTransformBenchmarkStopsActionSequence(t *testing.T) {
	type testCase struct {
		name   string
		result pipeline.ActionResult
	}
	tests := []testCase{
		{
			name:   "break",
			result: pipeline.ActionBreak,
		},
		{
			name:   "discard",
			result: pipeline.ActionDiscard,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			steps := []transformCallStep{
				{
					run: func(*pipeline.Event) pipeline.ActionResult {
						return tc.result
					},
				},
				{
					run: func(*pipeline.Event) pipeline.ActionResult {
						called = true
						return pipeline.ActionPass
					},
				},
			}
			require.Equal(t, tc.result, runTransformCallSteps(&pipeline.Event{}, steps))
			require.False(t, called)
		})
	}
}

func TestTransformBenchmarkObservesExecutionErrors(t *testing.T) {
	scenario := transformCallScenario{
		source: `.out = trim_right(.source, "x")`,
		inputs: []transformCallCase{
			{
				name:  "wrong_type",
				input: `{"source":42}`,
			},
		},
	}
	logCore, logs := observer.New(zap.ErrorLevel)
	variant := scenario.variants(t)[0]
	run := variant.runner(t, zap.New(logCore))
	events := newTransformCallEvents(t, 1)
	resetTransformCallEvents(t, events, scenario.inputs, 0)
	runTransformCallEvents(events, run)
	require.Equal(t, pipeline.ActionPass, events[0].result)
	require.NotEmpty(t, logs.All(), "ActionPass must not hide a transform evaluation error")
}
