package transformbench

import (
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	transformruntime "github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"go.uber.org/zap"
)

// TestTransformStdlibRetention is opt-in and writes only diagnostic profiles.
// Run this test alone, with each mode/workload in a separate process on both
// revisions. Compare inuse stacks, not timing or exact process-wide heap sizes.
func TestTransformStdlibRetention(t *testing.T) {
	rounds := startRetentionDiagnostic(t)
	switch *retentionMode {
	case "runtime":
		probeStdlibRuntimeRetention(t, rounds)
	case "plugin":
		probeStdlibPluginRetention(t, rounds)
	case "slice":
		probeStdlibSliceRetention(t, rounds)
	default:
		t.Fatalf("unknown stdlib retention mode %q", *retentionMode)
	}
}

type retentionStdlibPrograms struct {
	join   *core.CallExpr
	format *core.CallExpr
}

type retentionStdlibPosition struct{}

func (retentionStdlibPosition) String() string { return "stdlib-retention:1:1" }

func retentionStdlibCall(t *testing.T, name string, args []core.Argument) *core.CallExpr {
	t.Helper()
	fn, ok := stdlib.GetRegistry().Get(name)
	if !ok {
		t.Fatalf("missing function %s", name)
	}
	plan := fn.PrepareCall(args)
	if plan == nil {
		t.Fatalf("missing prepared call for %s", name)
	}
	return &core.CallExpr{
		Node: core.NewNode(retentionStdlibPosition{}),
		Name: name,
		Args: args,
		Plan: plan,
	}
}

func probeStdlibRuntimeRetention(t *testing.T, rounds int) {
	programs := retentionStdlibPrograms{
		join: retentionStdlibCall(t, "join", []core.Argument{
			{
				Value: &core.IdentExpr{Name: "values"},
			},
			{
				Value: &core.ConstExpr{V: core.StringValue{V: ":"}},
			},
		}),
		format: retentionStdlibCall(t, "to_string", []core.Argument{
			{
				Value: &core.IdentExpr{Name: "values"},
			},
		}),
	}
	ctx := transformruntime.NewContext(nil, stdlib.GetRegistry())
	for range retentionSmallEvents {
		runRetentionStdlibCall(t, ctx, programs, 4, false)
	}
	warm := retentionMemStats()
	for range rounds {
		// A large payload at the tail checks stale references in retained
		// capacity. The oversized call must not replace the bounded cache.
		runRetentionStdlibCall(t, ctx, programs, 64, true)
		runRetentionStdlibCall(t, ctx, programs, 256, true)
		for range retentionSmallEvents {
			runRetentionStdlibCall(t, ctx, programs, 4, false)
		}
	}
	retained := writeRetentionSnapshot(t, "stdlib_retained", warm)
	runtime.KeepAlive(ctx)
	runtime.KeepAlive(programs)

	ctx = nil
	writeRetentionSnapshot(t, "stdlib_released", retained)
	runtime.KeepAlive(ctx)
	runtime.KeepAlive(programs)
}

// Input values and validation strings disappear before the caller takes a
// snapshot. Plans reference variables, so they cannot own a large payload.
func runRetentionStdlibCall(t *testing.T, ctx *transformruntime.Context, programs retentionStdlibPrograms, count int, large bool) {
	t.Helper()
	defer ctx.Reset()
	parts := retentionStdlibParts(count, large)
	values := make([]core.Value, len(parts))
	for i, part := range parts {
		values[i] = core.StringValue{V: part}
	}
	array := core.ArrayValue{V: values}
	ctx.SetVar("values", array)
	retentionStdlibEval(t, ctx, programs.join, strings.Join(parts, ":"))
	formatted := retentionStdlibArrayText(parts)
	retentionStdlibEval(t, ctx, programs.format, formatted)
	ctx.SetVar("values", core.ArrayValue{V: []core.Value{
		core.ArrayValue{},
		array,
		core.ObjectValue{},
	}})
	retentionStdlibEval(t, ctx, programs.format, "[[], "+formatted+", {}]")
}

func retentionStdlibEval(t *testing.T, ctx *transformruntime.Context, call *core.CallExpr, want string) {
	t.Helper()
	value, err := call.Eval(ctx)
	if err != nil {
		t.Fatal(err)
	}
	text, ok := value.(core.StringValue)
	if !ok || text.V != want {
		t.Fatalf("%s returned an unexpected result", call.Name)
	}
}

func retentionStdlibParts(count int, large bool) []string {
	parts := make([]string, count)
	for i := range parts {
		parts[i] = "small"
	}
	if large {
		parts[len(parts)-1] = strings.Repeat("x", retentionPayloadBytes)
	}
	return parts
}

func retentionStdlibArrayText(parts []string) string {
	quoted := make([]string, len(parts))
	for i, part := range parts {
		quoted[i] = strconv.Quote(part)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func probeStdlibPluginRetention(t *testing.T, rounds int) {
	plugin := &transform.Plugin{}
	plugin.Start(&transform.Config{Source: `
		.joined = join(.values, ":")
		.formatted = to_string(.values)
		.nested = to_string(.nested_input)
		.finished = .marker
	`}, &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{PipelineName: "stdlib_retention_diagnostic"},
		Logger:              zap.NewNop().Sugar(),
	})
	defer plugin.Stop()
	for range retentionSmallEvents {
		runRetentionStdlibPluginEvent(t, plugin, 4, false)
	}
	warm := retentionMemStats()
	for range rounds {
		runRetentionStdlibPluginEvent(t, plugin, 64, true)
		runRetentionStdlibPluginEvent(t, plugin, 256, true)
		for range retentionSmallEvents {
			runRetentionStdlibPluginEvent(t, plugin, 4, false)
		}
	}
	retained := writeRetentionSnapshot(t, "stdlib_retained", warm)
	runtime.KeepAlive(plugin)

	plugin.Stop()
	writeRetentionSnapshot(t, "stdlib_stopped", retained)
	runtime.KeepAlive(plugin)
}

func runRetentionStdlibPluginEvent(t *testing.T, plugin *transform.Plugin, count int, large bool) {
	t.Helper()
	parts := retentionStdlibParts(count, large)
	// Decode a small scaffold, then attach the payload directly. Do not return
	// roots to the global decoder pool: only execution retention is measured.
	scaffold := retentionStdlibArrayText(retentionStdlibParts(count, false))
	root := retentionRoot(t, `{"values":`+scaffold+`,"nested_input":[[],`+scaffold+`,{}],"marker":"ok"}`)
	nested := root.Dig("nested_input").AsArray()[1].AsArray()
	for i, node := range root.Dig("values").AsArray() {
		node.MutateToString(parts[i])
		nested[i].MutateToString(parts[i])
	}
	event := &pipeline.Event{Root: root}
	if plugin.Do(event) != pipeline.ActionPass {
		t.Fatal("unexpected plugin result")
	}
	formatted := retentionStdlibArrayText(parts)
	type expectedField struct {
		name string
		text string
	}
	fields := []expectedField{
		{
			name: "joined",
			text: strings.Join(parts, ":"),
		},
		{
			name: "formatted",
			text: formatted,
		},
		{
			name: "nested",
			text: "[[], " + formatted + ", {}]",
		},
		{
			name: "finished",
			text: "ok",
		},
	}
	for _, field := range fields {
		node := root.Dig(field.name)
		if node == nil || node.AsString() != field.text {
			t.Fatalf("plugin returned an unexpected %s field", field.name)
		}
	}
}

func probeStdlibSliceRetention(t *testing.T, rounds int) {
	ctx := transformruntime.NewContext(nil, stdlib.GetRegistry())
	call := retentionStdlibCall(t, "slice", []core.Argument{
		{
			Value: &core.IdentExpr{Name: "text"},
		},
		{
			Value: &core.ConstExpr{V: core.IntegerValue{V: 0}},
		},
		{
			Name:  "end",
			Value: &core.ConstExpr{V: core.IntegerValue{V: 8}},
		},
	})
	for range retentionSmallEvents {
		runRetentionSliceInputs(t, ctx, call, 64)
	}
	warm := retentionMemStats()
	results := runRetentionSliceInputs(t, ctx, call, 64)
	for range rounds {
		results = append(results, runRetentionSliceInputs(t, ctx, call, retentionPayloadBytes)...)
		for range retentionSmallEvents {
			runRetentionSliceInputs(t, ctx, call, 64)
		}
	}
	retained := writeRetentionSnapshot(t, "slice_results_held", warm)
	runtime.KeepAlive(results)
	runtime.KeepAlive(ctx)
	runtime.KeepAlive(call)

	results = nil
	released := writeRetentionSnapshot(t, "slice_results_released", retained)
	runtime.KeepAlive(results)
	runtime.KeepAlive(ctx)
	runtime.KeepAlive(call)
	ctx = nil
	writeRetentionSnapshot(t, "slice_execution_released", released)
	runtime.KeepAlive(ctx)
	runtime.KeepAlive(call)
}

func runRetentionSliceInputs(t *testing.T, ctx *transformruntime.Context, call *core.CallExpr, size int) []string {
	t.Helper()
	type sliceInput struct {
		prefix string
		want   string
	}
	inputs := []sliceInput{
		{
			prefix: "slice-ok",
			want:   "slice-ok",
		},
		{
			prefix: "абвгдежз",
			want:   "абвгдежз",
		},
		{
			prefix: "a\xffb\x80c\xfede",
			want:   "a�b�c�de",
		},
	}
	results := make([]string, 0, len(inputs))
	for _, input := range inputs {
		text := input.prefix + strings.Repeat("x", size-len(input.prefix))
		ctx.SetVar("text", core.StringValue{V: text})
		value, err := call.Eval(ctx)
		if err != nil {
			t.Fatal(err)
		}
		result, ok := value.(core.StringValue)
		if !ok || result.V != input.want {
			t.Fatal("slice returned an unexpected short result")
		}
		results = append(results, result.V)
		ctx.Reset()
	}
	return results
}
