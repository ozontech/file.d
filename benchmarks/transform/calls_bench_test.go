package transformbench

import (
	"testing"

	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

const transformCallsBatchSize = 256

type transformCallEvent struct {
	event  pipeline.Event
	result pipeline.ActionResult
}

func newTransformCallEvents(tb testing.TB, count int) []transformCallEvent {
	tb.Helper()
	events := make([]transformCallEvent, count)
	for i := range events {
		events[i].event = pipeline.Event{Root: insaneJSON.Spawn(), SourceName: transformCallsSourceName}
	}
	tb.Cleanup(func() {
		for i := range events {
			insaneJSON.Release(events[i].event.Root)
		}
	})
	return events
}

func resetTransformCallEvents(tb testing.TB, events []transformCallEvent, input string) {
	tb.Helper()
	for i := range events {
		if err := events[i].event.Root.DecodeString(input); err != nil {
			tb.Fatal(err)
		}
	}
}

func runTransformCallEvents(events []transformCallEvent, run func(*pipeline.Event) pipeline.ActionResult) {
	for i := range events {
		events[i].result = run(&events[i].event)
	}
}

// BenchmarkTransformCalls measures Do on already decoded events.
// trim4 compares the same four trims; full compares the same result with
// before in transform and a regex prefix extraction in modify. Parity only
// covers valid string inputs. Setup and reset are excluded from all metrics.
// Scenario and variant slices keep execution and output order stable.
func BenchmarkTransformCalls(b *testing.B) {
	for _, scenario := range transformCallScenarios() {
		b.Run(scenario.name, func(b *testing.B) {
			for _, variant := range []string{"transform", "modify"} {
				b.Run(variant, func(b *testing.B) {
					b.StopTimer()
					// Successful runs produce no log entries or logger allocations;
					// errors from any measured iteration invalidate the benchmark.
					logCore, logs := observer.New(zap.ErrorLevel)
					run := scenario.runner(b, variant, zap.New(logCore))
					events := newTransformCallEvents(b, min(b.N, transformCallsBatchSize))

					// Warm the plugin and every Root before measuring steady reuse.
					resetTransformCallEvents(b, events, scenario.input)
					runTransformCallEvents(events, run)
					require.Empty(b, logs.All(), "unexpected plugin errors")
					for i := range events {
						scenario.checkResult(b, events[i].event.Root, events[i].result)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for completed := 0; completed < b.N; {
						batch := events[:min(len(events), b.N-completed)]
						// Batch resets keep timer transitions outside the per-event
						// loop. StartTimer/StopTimer also isolate allocation counts.
						resetTransformCallEvents(b, batch, scenario.input)
						b.StartTimer()
						runTransformCallEvents(batch, run)
						b.StopTimer()
						if logs.Len() != 0 {
							b.Fatalf("unexpected plugin errors: %v", logs.All())
						}
						completed += len(batch)
					}
					for i := range events {
						scenario.checkResult(b, events[i].event.Root, events[i].result)
					}
				})
			}
		})
	}
}
