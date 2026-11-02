package transformbench

import (
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	transformruntime "github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/require"
)

// Join workloads are separate from the paired report and GROUP/CASE selection.
// Do includes JSON materialization and writing the result to the event;
// Prepared isolates the call with an already materialized array.
func BenchmarkTransformJoinDo(b *testing.B) {
	scenarios := transformJoinScenarios()
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

func BenchmarkTransformJoinPrepared(b *testing.B) {
	counts := transformJoinElementCounts()
	for _, count := range counts {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			benchmarkTransformJoinPrepared(b, count, nil)
		})
	}
}

// AfterLarge measures short joins after a large call has grown reusable storage.
func BenchmarkTransformJoinAfterLarge(b *testing.B) {
	b.Run("Prepared", func(b *testing.B) {
		benchmarkTransformJoinPrepared(b, 4, transformJoinSamples(64))
	})
	b.Run("Do", func(b *testing.B) {
		scenario := transformJoinScenario(4)
		warmup := transformJoinScenario(64)
		variants := scenario.variants(b)
		require.Len(b, variants, 1)
		benchmarkTransformCallVariantWithWarmup(b, scenario.inputs, variants[0], warmup.inputs)
	})
}

func benchmarkTransformJoinPrepared(b *testing.B, count int, warmups []transformJoinSample) {
	b.Helper()
	b.StopTimer()
	registry := stdlib.GetRegistry()
	fn, ok := registry.Get("join")
	require.True(b, ok)
	ctx := transformruntime.NewContext(nil, registry)
	b.Cleanup(ctx.Reset)
	samples := slices.Clone(warmups)
	samples = append(samples, transformJoinSamples(count)...)
	calls := make([]*core.CallExpr, len(samples))
	results := make([]core.Value, len(samples))
	for i, sample := range samples {
		elements := make([]core.Value, len(sample.parts))
		for j, part := range sample.parts {
			elements[j] = core.StringValue{V: part}
		}
		args := []core.Argument{
			{
				Value: &core.ConstExpr{V: core.ArrayValue{V: elements}},
			},
			{
				Value: &core.ConstExpr{V: core.StringValue{V: sample.separator}},
			},
		}
		calls[i] = &core.CallExpr{
			Node: core.NewNode(transformJoinBenchmarkPosition{}),
			Name: fn.Name(),
			Args: args,
			Plan: fn.PrepareCall(args),
		}
		require.NotNil(b, calls[i].Plan)
		result, err := calls[i].Eval(ctx)
		require.NoError(b, err)
		results[i] = result
		require.Equal(b, core.StringValue{V: sample.want}, results[i])
	}

	// Exclude optional warmup calls from the measured corpus.
	calls = calls[len(warmups):]
	results = results[len(warmups):]
	samples = samples[len(warmups):]

	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		index := i % len(calls)
		result, err := calls[index].Eval(ctx)
		if err != nil {
			b.Fatal(err)
		}
		results[index] = result
	}
	b.StopTimer()
	for i, sample := range samples {
		require.Equal(b, core.StringValue{V: sample.want}, results[i])
	}
}

func TestTransformJoinScenarios(t *testing.T) {
	scenarios := transformJoinScenarios()
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

type transformJoinBenchmarkPosition struct{}

func (transformJoinBenchmarkPosition) String() string { return "join-benchmark:1:1" }

func transformJoinElementCounts() []int {
	return []int{0, 1, 4, 16, 64, 256, 1024}
}

type transformJoinSample struct {
	name      string
	parts     []string
	separator string
	want      string
}

func transformJoinSamples(count int) []transformJoinSample {
	type samplePattern struct {
		name      string
		words     []string
		separator string
	}
	patterns := []samplePattern{
		{
			name:      "ascii",
			words:     []string{"api", "request", "", "completed"},
			separator: ",",
		},
		{
			name:      "escaped_unicode",
			words:     []string{"запрос", "界🙂", "line\nnext", `"quoted"\path`},
			separator: " | ",
		},
	}
	samples := make([]transformJoinSample, 0, len(patterns))
	for _, pattern := range patterns {
		parts := make([]string, count)
		for i := range parts {
			parts[i] = pattern.words[i%len(pattern.words)]
		}
		samples = append(samples, transformJoinSample{
			name:      pattern.name,
			parts:     parts,
			separator: pattern.separator,
			want:      strings.Join(parts, pattern.separator),
		})
	}
	return samples
}

func transformJoinScenarios() []transformCallScenario {
	counts := transformJoinElementCounts()
	scenarios := make([]transformCallScenario, 0, len(counts))
	for _, count := range counts {
		scenarios = append(scenarios, transformJoinScenario(count))
	}
	return scenarios
}

func transformJoinScenario(count int) transformCallScenario {
	samples := transformJoinSamples(count)
	inputs := make([]transformCallCase, 0, len(samples))
	for _, sample := range samples {
		inputs = append(inputs, transformEventCase(sample.name,
			map[string]any{
				"parts":     sample.parts,
				"separator": sample.separator,
				"out":       "stale",
				"keep":      true,
			},
			map[string]any{
				"parts":     sample.parts,
				"separator": sample.separator,
				"out":       sample.want,
				"keep":      true,
			},
		))
	}
	return transformCallScenario{
		name: strconv.Itoa(count),
		source: `
			.out = join(.parts, .separator)
		`,
		inputs: inputs,
	}
}
