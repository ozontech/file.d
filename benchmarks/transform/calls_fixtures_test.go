package transformbench

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/modify"
	"github.com/ozontech/file.d/plugin/action/transform"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const transformCallsSourceName = "transform_calls"

type transformCallScenario struct {
	name         string
	input        string
	source       string
	modifyConfig modify.Config
	want         string
	// Additional correctness cases use the same plugins but are not benchmarked.
	cases []transformCallCase
}

type transformCallCase struct {
	name  string
	input string
	want  string
}

// Expected values are fixed independently of either plugin. Slice order is
// also the execution and output order of the paired benchmarks.
func transformCallScenarios() []transformCallScenario {
	return []transformCallScenario{
		{
			name:  "assign_new5",
			input: `{"source":"x"}`,
			source: `
				.a = "v1"
				.b = "v2"
				.c = "v3"
				.d = "v4"
				.e = "v5"
			`,
			modifyConfig: modify.Config{"a": "v1", "b": "v2", "c": "v3", "d": "v4", "e": "v5"},
			want:         `{"source":"x","a":"v1","b":"v2","c":"v3","d":"v4","e":"v5"}`,
		},
		{
			name:  "assign_existing5",
			input: `{"source":"x","a":"old","b":"old","c":"old","d":"old","e":"old"}`,
			source: `
				.a = "v1"
				.b = "v2"
				.c = "v3"
				.d = "v4"
				.e = "v5"
			`,
			modifyConfig: modify.Config{"a": "v1", "b": "v2", "c": "v3", "d": "v4", "e": "v5"},
			want:         `{"source":"x","a":"v1","b":"v2","c":"v3","d":"v4","e":"v5"}`,
		},
		{
			name:         "trim_one",
			input:        `{"source":"api123"}`,
			source:       `.out = trim_right(.source, "0123456789")`,
			modifyConfig: modify.Config{"out": `${source|trim("right","0123456789")}`},
			want:         `{"source":"api123","out":"api"}`,
			cases: []transformCallCase{
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","out":""}`,
				},
				{
					name:  "preserve_extra_fields",
					input: `{"source":"api123","count":2,"enabled":true,"tags":["edge"],"meta":{"zone":"a"}}`,
					want:  `{"source":"api123","out":"api","count":2,"enabled":true,"tags":["edge"],"meta":{"zone":"a"}}`,
				},
			},
		},
		{
			name:  "trim4",
			input: `{"source":"api-123z45"}`,
			source: `
				host = trim_right(.source, "0123456789")
				.cluster_name = trim_right(
					trim_right(
						trim_right(host, "z"),
						"0123456789"
					),
					"-"
				)
			`,
			modifyConfig: modify.Config{
				"cluster_name": `${source` +
					`|trim("right","0123456789")` +
					`|trim("right","z")` +
					`|trim("right","0123456789")` +
					`|trim("right","-")}`,
			},
			want: `{"source":"api-123z45","cluster_name":"api"}`,
			cases: []transformCallCase{
				{
					name:  "domain",
					input: `{"source":"api-123z45.example.net"}`,
					want:  `{"source":"api-123z45.example.net","cluster_name":"api-123z45.example.net"}`,
				},
				{
					name:  "multiple_dots",
					input: `{"source":"api-123z45.a.b"}`,
					want:  `{"source":"api-123z45.a.b","cluster_name":"api-123z45.a.b"}`,
				},
				{
					name:  "no_dot",
					input: `{"source":"api-123z45"}`,
					want:  `{"source":"api-123z45","cluster_name":"api"}`,
				},
				{
					name:  "leading_dot",
					input: `{"source":".domain"}`,
					want:  `{"source":".domain","cluster_name":".domain"}`,
				},
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","cluster_name":""}`,
				},
				{
					name:  "newline",
					input: `{"source":"api\nnode-123z45.domain"}`,
					want:  `{"source":"api\nnode-123z45.domain","cluster_name":"api\nnode-123z45.domain"}`,
				},
				{
					name:  "z_before_digits",
					input: `{"source":"api-z123.domain"}`,
					want:  `{"source":"api-z123.domain","cluster_name":"api-z123.domain"}`,
				},
				{
					name:  "only_digits",
					input: `{"source":"123"}`,
					want:  `{"source":"123","cluster_name":""}`,
				},
				{
					name:  "multiple_dashes",
					input: `{"source":"api--123z45.domain"}`,
					want:  `{"source":"api--123z45.domain","cluster_name":"api--123z45.domain"}`,
				},
			},
		},
		{
			name:  "full",
			input: `{"source":"api-123z45.example.net"}`,
			source: `
				host = trim_right(
					before(.source, "."),
					"0123456789"
				)
				.cluster_name = trim_right(
					trim_right(
						trim_right(host, "z"),
						"0123456789"
					),
					"-"
				)
			`,
			modifyConfig: modify.Config{
				"cluster_name": `${source` +
					`|re("^([^.]*)",1,[1],"")` +
					`|trim("right","0123456789")` +
					`|trim("right","z")` +
					`|trim("right","0123456789")` +
					`|trim("right","-")}`,
			},
			want: `{"source":"api-123z45.example.net","cluster_name":"api"}`,
			cases: []transformCallCase{
				{
					name:  "domain",
					input: `{"source":"api-123z45.example.net"}`,
					want:  `{"source":"api-123z45.example.net","cluster_name":"api"}`,
				},
				{
					name:  "multiple_dots",
					input: `{"source":"api-123z45.a.b"}`,
					want:  `{"source":"api-123z45.a.b","cluster_name":"api"}`,
				},
				{
					name:  "no_dot",
					input: `{"source":"api-123z45"}`,
					want:  `{"source":"api-123z45","cluster_name":"api"}`,
				},
				{
					name:  "leading_dot",
					input: `{"source":".domain"}`,
					want:  `{"source":".domain","cluster_name":""}`,
				},
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","cluster_name":""}`,
				},
				{
					name:  "newline",
					input: `{"source":"api\nnode-123z45.domain"}`,
					want:  `{"source":"api\nnode-123z45.domain","cluster_name":"api\nnode"}`,
				},
				{
					name:  "z_before_digits",
					input: `{"source":"api-z123.domain"}`,
					want:  `{"source":"api-z123.domain","cluster_name":"api"}`,
				},
				{
					name:  "only_digits",
					input: `{"source":"123"}`,
					want:  `{"source":"123","cluster_name":""}`,
				},
				{
					name:  "multiple_dashes",
					input: `{"source":"api--123z45.domain"}`,
					want:  `{"source":"api--123z45.domain","cluster_name":"api"}`,
				},
			},
		},
	}
}

func (s *transformCallScenario) runner(tb testing.TB, variant string, logger *zap.Logger) func(*pipeline.Event) pipeline.ActionResult {
	tb.Helper()
	switch variant {
	case "transform":
		p := &transform.Plugin{}
		p.Start(&transform.Config{Source: s.source}, &pipeline.ActionPluginParams{
			Logger:              logger.Sugar(),
			PluginDefaultParams: pipeline.PluginDefaultParams{PipelineName: "benchmark_transform_" + s.name},
		})
		tb.Cleanup(p.Stop)
		return p.Do
	case "modify":
		require.NotNil(tb, s.modifyConfig)
		p := &modify.Plugin{}
		p.Start(&s.modifyConfig, &pipeline.ActionPluginParams{Logger: logger.Sugar()})
		tb.Cleanup(p.Stop)
		return p.Do
	default:
		tb.Fatalf("unknown benchmark variant %q", variant)
		return nil
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

func (s *transformCallScenario) checkResult(tb testing.TB, root *insaneJSON.Root, result pipeline.ActionResult) {
	tb.Helper()
	require.Equal(tb, pipeline.ActionPass, result)
	// Compare the entire event, including types, to catch extra fields, missing
	// fields and changes to preserved data. All serialization stays outside timers.
	require.Equal(tb, decodeTransformCallFields(tb, s.want), decodeTransformCallFields(tb, root.EncodeToString()))
}

// TestTransformBenchmarkScenarios checks every benchmark fixture and verifies
// that full and partial batches restore the original input before each call.
func TestTransformBenchmarkScenarios(t *testing.T) {
	for _, scenario := range transformCallScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			for _, variant := range []string{"transform", "modify"} {
				t.Run(variant, func(t *testing.T) {
					logCore, logs := observer.New(zap.ErrorLevel)
					run := scenario.runner(t, variant, zap.New(logCore))
					events := newTransformCallEvents(t, 3)
					input := decodeTransformCallFields(t, scenario.input)
					// Exercise full and partial batches through the same reset/run
					// helpers used by the benchmark. Inputs must be fresh each time.
					for _, size := range []int{3, 2, 3} {
						batch := events[:size]
						resetTransformCallEvents(t, batch, scenario.input)
						runTransformCallEvents(batch, func(event *pipeline.Event) pipeline.ActionResult {
							require.Equal(t, input, decodeTransformCallFields(t, event.Root.EncodeToString()))
							return run(event)
						})
						// Do returns ActionPass even when evaluation fails.
						require.Empty(t, logs.All(), "unexpected plugin errors")
						for i := range batch {
							scenario.checkResult(t, batch[i].event.Root, batch[i].result)
						}
					}
				})
			}
		})
	}
}

// TestTransformBenchmarkCases checks each scenario's additional inputs using
// the same plugin and Root repeatedly. These cases are not extra benchmarks.
func TestTransformBenchmarkCases(t *testing.T) {
	for _, scenario := range transformCallScenarios() {
		if len(scenario.cases) == 0 {
			continue
		}
		t.Run(scenario.name, func(t *testing.T) {
			for _, variant := range []string{"transform", "modify"} {
				t.Run(variant, func(t *testing.T) {
					logCore, logs := observer.New(zap.ErrorLevel)
					run := scenario.runner(t, variant, zap.New(logCore))
					root := insaneJSON.Spawn()
					defer insaneJSON.Release(root)
					event := &pipeline.Event{Root: root, SourceName: transformCallsSourceName}
					for _, tc := range scenario.cases {
						t.Run(tc.name, func(t *testing.T) {
							fixture := scenario
							fixture.input = tc.input
							fixture.want = tc.want
							// Reuse both the plugin and Root across different inputs,
							// decoding fresh contents before every execution.
							for range 3 {
								require.NoError(t, root.DecodeString(fixture.input))
								result := run(event)
								require.Empty(t, logs.All(), "unexpected plugin errors")
								fixture.checkResult(t, root, result)
							}
						})
					}
				})
			}
		})
	}
}
