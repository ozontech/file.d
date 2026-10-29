package transformbench

import (
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const transformCallsBatchSize = 256

// BenchmarkTransformCalls measures Do on already decoded events.
// Every pair uses equivalent inputs and the same regex/no-regex approach.
// Each fixture declares its input domain. Setup and reset are excluded from
// all metrics.
// Scenario and variant slices keep execution and output order stable.
func BenchmarkTransformCalls(b *testing.B) {
	if len(selectedTransformBenchmarks.calls) == 0 {
		b.Skip("no paired scenarios selected")
	}
	for _, scenario := range selectedTransformBenchmarks.calls {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 2, "paired benchmark requires a comparator")
			for _, variant := range variants {
				b.Run(variant.name, func(b *testing.B) {
					benchmarkTransformCallVariant(b, scenario.inputs, variant)
				})
			}
		})
	}
}

// BenchmarkTransformOnly tracks workloads without a valid modify equivalent.
// It uses the same per-event measurement boundary as the paired benchmarks.
func BenchmarkTransformOnly(b *testing.B) {
	if len(selectedTransformBenchmarks.only) == 0 {
		b.Skip("no standalone scenarios selected")
	}
	for _, scenario := range selectedTransformBenchmarks.only {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1, "standalone benchmark must not have a comparator")
			variant := variants[0]
			b.Run(variant.name, func(b *testing.B) {
				benchmarkTransformCallVariant(b, scenario.inputs, variant)
			})
		})
	}
}

// benchmarkTransformCallVariant is shared by paired and transform-only workloads.
func benchmarkTransformCallVariant(b *testing.B, inputs []transformCallCase, variant transformCallVariant) {
	b.Helper()
	b.StopTimer()
	require.NotEmpty(b, inputs)
	// Errors from any measured iteration invalidate the result, even when Do
	// logs the error and returns ActionPass.
	logCore, logs := observer.New(zap.ErrorLevel)
	run := variant.runner(b, zap.New(logCore))
	events := newTransformCallEvents(b, min(b.N, transformCallsBatchSize))

	// Warm every Root and every input, even if the corpus exceeds the batch or
	// the requested number of iterations. Warmup does not consume measured inputs.
	next := 0
	for remaining := max(len(events), len(inputs)); remaining > 0; {
		batch := events[:min(len(events), remaining)]
		next = resetTransformCallEvents(b, batch, inputs, next)
		runTransformCallEvents(batch, run)
		require.Empty(b, logs.All(), "unexpected plugin errors")
		for i := range batch {
			inputs[batch[i].inputIndex].checkResult(b, &batch[i])
		}
		remaining -= len(batch)
	}

	next = 0
	b.ReportAllocs()
	b.ResetTimer()
	for completed := 0; completed < b.N; {
		batch := events[:min(len(events), b.N-completed)]
		next = resetTransformCallEvents(b, batch, inputs, next)
		// StartTimer/StopTimer isolate allocation counts as well as duration.
		b.StartTimer()
		runTransformCallEvents(batch, run)
		b.StopTimer()
		if logs.Len() != 0 {
			b.Fatalf("unexpected plugin errors: %v", logs.All())
		}
		for i := range batch {
			if batch[i].result != pipeline.ActionPass {
				b.Fatalf("unexpected action result: %v", batch[i].result)
			}
		}
		completed += len(batch)
	}
	for i := range events {
		inputs[events[i].inputIndex].checkResult(b, &events[i])
	}
}
