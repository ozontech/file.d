package transformbench

import (
	"strconv"
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	transformruntime "github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/require"
)

// Slice workloads are separate from the paired report and GROUP/CASE selection.
// Prepared isolates the function call; Do also includes reading fields and
// writing the result to an already decoded event.
func BenchmarkTransformSlicePrepared(b *testing.B) {
	workloads := transformSliceWorkloads()
	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			benchmarkTransformSlicePrepared(b, workload.samples)
		})
	}
}

// Invalid UTF-8 must reach the call as raw Go strings: encoding these inputs as
// JSON would replace the invalid bytes before slice sees them. Sizes are bytes.
func BenchmarkTransformSliceInvalidPrepared(b *testing.B) {
	patterns := []string{"a\xff\xfe�界\x80b", "🙂\xc0\xaf\x80�x\xff"}
	sizes := []int{32, 4096}
	modes := []string{"prefix", "full"}
	for _, size := range sizes {
		for _, mode := range modes {
			samples := make([]transformSliceSample, 0, len(patterns))
			for i, pattern := range patterns {
				text := strings.Repeat(pattern, size/len(pattern)+1)[:size]
				runes := []rune(text)
				start, end := transformSliceBounds(len(runes), mode, i)
				samples = append(samples, transformSliceSample{
					name:   strconv.Itoa(i),
					source: text,
					start:  start,
					end:    end,
					want:   transformSliceExpected(runes, start, end),
				})
			}
			b.Run(strconv.Itoa(size)+"/"+mode, func(b *testing.B) {
				benchmarkTransformSlicePrepared(b, samples)
			})
		}
	}
}

func BenchmarkTransformSliceDo(b *testing.B) {
	workloads := transformSliceWorkloads()
	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			scenario := transformSliceScenario(workload)
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

func benchmarkTransformSlicePrepared(b *testing.B, samples []transformSliceSample) {
	b.Helper()
	b.StopTimer()
	registry := stdlib.GetRegistry()
	fn, ok := registry.Get("slice")
	require.True(b, ok)
	ctx := transformruntime.NewContext(nil, registry)
	b.Cleanup(ctx.Reset)
	calls := make([]*core.CallExpr, len(samples))
	results := make([]core.Value, len(samples))
	for i, sample := range samples {
		var end core.Value = core.NullValue{}
		if sample.end != nil {
			end = core.IntegerValue{V: *sample.end}
		}
		args := []core.Argument{
			{
				Value: &core.ConstExpr{V: core.StringValue{V: sample.source}},
			},
			{
				Value: &core.ConstExpr{V: core.IntegerValue{V: sample.start}},
			},
			{
				Name:  "end",
				Value: &core.ConstExpr{V: end},
			},
		}
		calls[i] = &core.CallExpr{
			Node: core.NewNode(transformSliceBenchmarkPosition{}),
			Name: fn.Name(),
			Args: args,
			Plan: fn.PrepareCall(args),
		}
		require.NotNil(b, calls[i].Plan)
		result, err := calls[i].Eval(ctx)
		require.NoError(b, err)
		results[i] = result
		require.Equal(b, core.StringValue{V: sample.want}, result)
	}

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

func TestTransformSliceScenarios(t *testing.T) {
	workloads := transformSliceWorkloads()
	for _, workload := range workloads {
		t.Run(workload.name, func(t *testing.T) {
			scenario := transformSliceScenario(workload)
			checkTransformCallScenario(t, &scenario)
		})
	}
}

type transformSliceBenchmarkPosition struct{}

func (transformSliceBenchmarkPosition) String() string { return "slice-benchmark:1:1" }

type transformSliceWorkload struct {
	name    string
	samples []transformSliceSample
}

type transformSliceSample struct {
	name   string
	source string
	start  int64
	end    *int64
	want   string
}

func transformSliceWorkloads() []transformSliceWorkload {
	type textPattern struct {
		name  string
		texts []string
	}
	patterns := []textPattern{
		{
			name:  "ascii",
			texts: []string{"request/result:ok/", "user=17 status=200\n"},
		},
		{
			name:  "unicode",
			texts: []string{"запрос界🙂/результат", "ответ🙂\nπαράδειγμα"},
		},
	}
	type inputSize struct {
		runes  int
		ranges []string
	}
	sizes := []inputSize{
		{
			runes:  32,
			ranges: []string{"prefix", "suffix", "middle", "empty"},
		},
		{
			runes:  4096,
			ranges: []string{"prefix", "suffix", "middle", "full"},
		},
	}
	var workloads []transformSliceWorkload
	for _, pattern := range patterns {
		for _, size := range sizes {
			for _, mode := range size.ranges {
				samples := make([]transformSliceSample, 0, len(pattern.texts))
				for i, text := range pattern.texts {
					patternRunes := []rune(text)
					runes := make([]rune, size.runes)
					for j := range runes {
						runes[j] = patternRunes[j%len(patternRunes)]
					}
					start, end := transformSliceBounds(size.runes, mode, i)
					samples = append(samples, transformSliceSample{
						name:   strconv.Itoa(i),
						source: string(runes),
						start:  start,
						end:    end,
						want:   transformSliceExpected(runes, start, end),
					})
				}
				workloads = append(workloads, transformSliceWorkload{
					name:    pattern.name + "/" + strconv.Itoa(size.runes) + "/" + mode,
					samples: samples,
				})
			}
		}
	}
	return workloads
}

// Each workload alternates two texts and two sets of bounds. Short selections
// contain 16 or 12 runes; middle also exercises a negative end position.
func transformSliceBounds(length int, mode string, sample int) (int64, *int64) {
	width := int64(16 - sample*4)
	switch mode {
	case "prefix":
		return 0, &width
	case "suffix":
		return -width, nil
	case "middle":
		start := int64(length/2 - 8 + sample)
		end := start + width
		if sample != 0 {
			end -= int64(length)
		}
		return start, &end
	case "empty":
		start := int64(length/2 + sample)
		end := start - int64(sample)
		return start, &end
	case "full":
		if sample != 0 {
			return 0, nil
		}
		end := int64(length + 1)
		return -end, &end
	default:
		panic("unknown slice workload: " + mode)
	}
}

// Keep the rune-based reference independent of the optimized stdlib function.
func transformSliceExpected(runes []rune, start int64, end *int64) string {
	length := int64(len(runes))
	clamp := func(index int64) int64 {
		if index < 0 {
			index += length
		}
		return min(max(index, 0), length)
	}
	first := clamp(start)
	last := length
	if end != nil {
		last = clamp(*end)
	}
	if last <= first {
		return ""
	}
	return string(runes[first:last])
}

func transformSliceScenario(workload transformSliceWorkload) transformCallScenario {
	inputs := make([]transformCallCase, 0, len(workload.samples))
	for _, sample := range workload.samples {
		inputs = append(inputs, transformEventCase(sample.name,
			map[string]any{
				"source": sample.source,
				"start":  sample.start,
				"end":    sample.end,
				"out":    "stale",
				"keep":   true,
			},
			map[string]any{
				"source": sample.source,
				"start":  sample.start,
				"end":    sample.end,
				"out":    sample.want,
				"keep":   true,
			},
		))
	}
	return transformCallScenario{
		name: workload.name,
		source: `
			.out = slice(.source, .start, end: .end)
		`,
		inputs: inputs,
	}
}
