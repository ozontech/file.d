package transformbench

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	transformruntime "github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

var (
	retentionProfileDir = flag.String("transform.retention-profile-dir", "", "opt-in retained heap diagnostic output directory")
	retentionMode       = flag.String("transform.retention-mode", "runtime", "retained heap diagnostic mode: runtime, plugin, or slice (stdlib diagnostic only)")
	retentionWorkload   = flag.String("transform.retention-workload", "small", "retained heap diagnostic workload: small, large_then_small, repeated_large_then_small")
)

const (
	retentionPayloadBytes = 8 * 1024 * 1024
	retentionSmallEvents  = 20
)

// TestTransformRuntimeRetention is a diagnostic, not a timing benchmark or a
// heap-size assertion. Run each mode/workload in a separate process, with the
// same toolchain and flags on both revisions, then compare inuse allocation
// stacks. Process initialization predates the opt-in sampling rate below.
func TestTransformRuntimeRetention(t *testing.T) {
	rounds := startRetentionDiagnostic(t)
	switch *retentionMode {
	case "runtime":
		probeRuntimeRetention(t, rounds)
	case "plugin":
		probePluginRetention(t, rounds)
	default:
		t.Fatalf("unknown runtime retention mode %q", *retentionMode)
	}
}

func startRetentionDiagnostic(t *testing.T) int {
	t.Helper()
	if *retentionProfileDir == "" {
		t.Skip("set -transform.retention-profile-dir to collect retained heap profiles")
	}
	previousRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = previousRate })
	previousProcs := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })

	var rounds int
	switch *retentionWorkload {
	case "small":
	case "large_then_small":
		rounds = 1
	case "repeated_large_then_small":
		rounds = 4
	default:
		t.Fatalf("unknown retention workload %q", *retentionWorkload)
	}
	if err := os.MkdirAll(*retentionProfileDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Logf("retention mode=%s workload=%s go=%s os=%s arch=%s procs=1 profile_rate=1 payload_bytes=%d large_rounds=%d small_events=%d",
		*retentionMode, *retentionWorkload, runtime.Version(), runtime.GOOS, runtime.GOARCH,
		retentionPayloadBytes, rounds, retentionSmallEvents)
	return rounds
}

type retentionRuntimeState struct {
	ctx    *transformruntime.Context
	target *transformruntime.RootTarget
}

func (s *retentionRuntimeState) acquire(root *insaneJSON.Root, sourceName string) {
	if s.ctx == nil {
		// A supplied map also works on the baseline, before lazy metadata.
		s.target = transformruntime.NewRootTarget(root, sourceName, make(map[string]string))
		s.ctx = transformruntime.NewContext(s.target, stdlib.GetRegistry())
	} else {
		s.target.Root = root
		s.target.SourceName = sourceName
	}
}

func (s *retentionRuntimeState) release() {
	type resetter interface {
		Reset()
	}
	ctx, hasContextReset := any(s.ctx).(resetter)
	target, hasTargetReset := any(s.target).(resetter)
	if hasContextReset && hasTargetReset {
		ctx.Reset()
		target.Reset()
		return
	}
	// The baseline creates execution state per event and retains none when idle.
	s.ctx = nil
	s.target = nil
}

type retentionPrograms struct {
	small core.Expr
	deep  core.Expr
	wide  core.Expr
}

func probeRuntimeRetention(t *testing.T, rounds int) {
	state := &retentionRuntimeState{}
	programs := retentionPrograms{
		small: retentionCall(1, 2),
		deep:  retentionCall(80, 64),
		wide:  retentionCall(1, 1024),
	}
	for range retentionSmallEvents {
		runRetentionRuntimeEvent(t, state, programs, false, 2)
	}
	warm := retentionMemStats()
	for range rounds {
		// Exercise both a discarded path buffer and a retained buffer whose
		// stale tail must release the large string after a shorter path.
		runRetentionRuntimeEvent(t, state, programs, true, 128)
		runRetentionRuntimeEvent(t, state, programs, true, 16)
	}
	for range retentionSmallEvents {
		runRetentionRuntimeEvent(t, state, programs, false, 2)
	}
	retained := writeRetentionSnapshot(t, "retained", warm)
	runtime.KeepAlive(state)
	runtime.KeepAlive(programs)

	state.ctx = nil
	state.target = nil
	writeRetentionSnapshot(t, "released", retained)
	runtime.KeepAlive(state)
	runtime.KeepAlive(programs)
}

// Both revisions support NewCallPlan. The artificial width-64/depth-80 plan
// reaches the retained frame limits; width 1024 must remain transient. Plans
// contain only a variable reference, never the event payload being audited.
func retentionCall(depth, width int) core.Expr {
	var expr core.Expr = &core.IdentExpr{Name: "payload"}
	for range depth {
		expr = &core.CallExpr{
			Name: "retention_identity",
			Args: []core.Argument{
				{Value: &core.IdentExpr{Name: "payload"}},
				{Value: expr},
			},
			Plan: core.NewCallPlan([]int{0, 1}, make([]core.Value, width), func(_ core.Position, values []core.Value) (core.Value, error) {
				return values[0], nil
			}),
		}
	}
	return expr
}

// The helper returns before GC so no local, result, closure, or error can keep
// a large event alive. Roots are deliberately not returned to insane-json's
// global pool: this probe measures runtime ownership rather than decoder pools.
func runRetentionRuntimeEvent(t *testing.T, state *retentionRuntimeState, programs retentionPrograms, large bool, pathLength int) {
	t.Helper()
	payload := "small payload"
	entries := 4
	if large {
		payload = strings.Repeat("x", retentionPayloadBytes)
		entries = 4096
	}
	root := retentionRoot(t, `{"items":[]}`)
	state.acquire(root, payload)
	defer state.release()
	state.ctx.SetVar("payload", core.StringValue{V: payload})
	for i := range entries {
		key := strconv.Itoa(i)
		state.ctx.SetVar(key, core.StringValue{V: payload})
		path := core.Path{Root: core.MetadataRoot, Segments: []core.Segment{core.FieldSeg(key)}}
		if err := state.target.Set(path, core.StringValue{V: payload}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range entries - 1 {
		key := strconv.Itoa(i)
		state.ctx.DeleteVar(key)
		path := core.Path{Root: core.MetadataRoot, Segments: []core.Segment{core.FieldSeg(key)}}
		if err := state.target.Delete(path); err != nil {
			t.Fatal(err)
		}
	}
	segments := make([]core.Segment, pathLength)
	for i := range segments {
		segments[i] = core.FieldSeg(payload[:8])
	}
	if _, err := state.target.Get(core.Path{Segments: segments}); err != nil {
		t.Fatal(err)
	}
	if _, err := state.target.Get(retentionFieldPath("missing")); err != nil {
		t.Fatal(err)
	}
	if large {
		retentionEval(t, programs.deep, state.ctx, len(payload))
		retentionEval(t, programs.wide, state.ctx, len(payload))
	} else {
		retentionEval(t, programs.small, state.ctx, len(payload))
	}
	value := core.ArrayValue{V: []core.Value{core.StringValue{V: payload}}}
	if err := state.target.Set(retentionFieldPath("output"), value); err != nil {
		t.Fatal(err)
	}
	if got := root.Dig("output").AsArray()[0].AsString(); len(got) != len(payload) {
		t.Fatalf("serialized payload length %d, want %d", len(got), len(payload))
	}
	if large {
		invalid := core.Path{Segments: []core.Segment{core.FieldSeg("items"), core.IndexSeg(1)}}
		if err := state.target.Set(invalid, value); err == nil || !strings.Contains(err.Error(), "out of bounds") {
			t.Fatalf("expected invalid-index serialization path, got %v", err)
		}
		unsupported := core.ArrayValue{V: []core.Value{core.StringValue{V: payload}, core.RegexValue{}}}
		if err := state.target.Set(retentionFieldPath("invalid"), unsupported); err == nil || !strings.Contains(err.Error(), "cannot serialize regex") {
			t.Fatalf("expected partial serialization failure, got %v", err)
		}
	}
}

func retentionEval(t *testing.T, expr core.Expr, ctx *transformruntime.Context, length int) {
	t.Helper()
	value, err := expr.Eval(ctx)
	if err != nil {
		t.Fatal(err)
	}
	text, ok := value.(core.StringValue)
	if !ok || len(text.V) != length {
		t.Fatal("prepared identity call returned an unexpected value")
	}
}

func probePluginRetention(t *testing.T, rounds int) {
	deepCall := strings.Repeat("trim_left(", 80) + "held" + strings.Repeat(", \" \")", 80)
	source := `
		held = .payload + "";
		%held = held
		.ignored = .missing[held]
		if .large { .nested = ` + deepCall + ` } else { .nested = trim_left(held, " ") }
		.groups = parse_regex_all(held, r'x', limit: 1, group: 0)
		.output = [held, .nested]
		.finished = .marker
	`
	plugin := &transform.Plugin{}
	plugin.Start(&transform.Config{Source: source}, &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{PipelineName: "retention_diagnostic"},
		Logger:              zap.NewNop().Sugar(),
	})
	defer plugin.Stop()
	for range retentionSmallEvents {
		runRetentionPluginEvent(t, plugin, false)
	}
	warm := retentionMemStats()
	for range rounds * 2 {
		runRetentionPluginEvent(t, plugin, true)
	}
	for range retentionSmallEvents {
		runRetentionPluginEvent(t, plugin, false)
	}
	retained := writeRetentionSnapshot(t, "retained", warm)
	runtime.KeepAlive(plugin)

	plugin.Stop()
	writeRetentionSnapshot(t, "stopped", retained)
	runtime.KeepAlive(plugin)
}

func runRetentionPluginEvent(t *testing.T, plugin *transform.Plugin, large bool) {
	t.Helper()
	payload := "small x payload"
	if large {
		payload = strings.Repeat("x", retentionPayloadBytes)
	}
	root := retentionRoot(t, `{"payload":"","large":false,"marker":"ok"}`)
	root.Dig("payload").MutateToString(payload)
	root.Dig("large").MutateToBool(large)
	event := &pipeline.Event{
		Root:       root,
		SourceName: payload,
	}
	if plugin.Do(event) != pipeline.ActionPass {
		t.Fatal("unexpected plugin result")
	}
	finished := root.Dig("finished")
	output := root.Dig("output")
	nested := root.Dig("nested")
	if finished == nil || finished.AsString() != "ok" || output == nil || len(output.AsArray()) != 2 || nested == nil || len(nested.AsString()) != len(payload) {
		t.Fatal("plugin did not complete the retention workload")
	}
}

func retentionRoot(t *testing.T, input string) *insaneJSON.Root {
	t.Helper()
	root, err := insaneJSON.DecodeString(input)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func retentionFieldPath(name string) core.Path {
	return core.Path{Segments: []core.Segment{core.FieldSeg(name)}}
}

func retentionMemStats() runtime.MemStats {
	// Heap profiles can lag collection by two GC cycles. No events or roots
	// remain in the caller's stack while these collections run.
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}

func writeRetentionSnapshot(t *testing.T, phase string, previous runtime.MemStats) runtime.MemStats {
	t.Helper()
	stats := retentionMemStats()
	path := filepath.Join(*retentionProfileDir, fmt.Sprintf("%s_%s_%s.heap", *retentionMode, *retentionWorkload, phase))
	t.Logf("phase=%s heap_alloc_bytes=%d heap_objects=%d heap_alloc_delta_bytes=%d heap_objects_delta=%d total_alloc_delta_bytes=%d profile=%s",
		phase, stats.HeapAlloc, stats.HeapObjects,
		int64(stats.HeapAlloc)-int64(previous.HeapAlloc), int64(stats.HeapObjects)-int64(previous.HeapObjects),
		stats.TotalAlloc-previous.TotalAlloc, path)
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := pprof.WriteHeapProfile(file); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return stats
}
