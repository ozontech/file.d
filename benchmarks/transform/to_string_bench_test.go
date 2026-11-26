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

// These workloads isolate composite formatting with scalar controls, without
// extending GROUP/CASE or the default report. Prepared uses native values;
// Do includes materializing fields and writing the result back to the event.
func BenchmarkTransformToStringPrepared(b *testing.B) {
	groups := []transformToStringGroup{
		{
			name:    "array",
			samples: transformToStringArraySamples,
		},
		{
			name:    "object",
			samples: transformToStringObjectSamples,
		},
	}
	workloads := transformToStringWorkloads(groups)
	for _, workload := range workloads {
		b.Run(workload.name, func(b *testing.B) {
			benchmarkTransformToStringPrepared(b, workload.samples, nil)
		})
	}
}

func BenchmarkTransformToStringDo(b *testing.B) {
	scenarios := transformToStringScenarios()
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

// AfterLarge measures four-element formatting after join has warmed the same
// execution's string-reference cache with a 64-element array.
func BenchmarkTransformToStringAfterLarge(b *testing.B) {
	b.Run("Prepared", func(b *testing.B) {
		benchmarkTransformToStringPrepared(b,
			transformToStringArraySamples(4), transformToStringArraySamples(64))
	})
	b.Run("Do", func(b *testing.B) {
		scenario := transformCallScenario{
			source: `
				if .warmup {
					.out = join(.value, ":")
				} else {
					.out = to_string(.value)
				}
			`,
			inputs: transformToStringAfterLargeInputs(4, false),
		}
		warmup := transformToStringAfterLargeInputs(64, true)
		variants := scenario.variants(b)
		require.Len(b, variants, 1)
		benchmarkTransformCallVariantWithWarmup(b, scenario.inputs, variants[0], warmup)
	})
}

func benchmarkTransformToStringPrepared(
	b *testing.B,
	samples []transformToStringSample,
	warmups []transformToStringSample,
) {
	b.Helper()
	b.StopTimer()
	registry := stdlib.GetRegistry()
	fn, ok := registry.Get("to_string")
	require.True(b, ok)
	ctx := transformruntime.NewContext(nil, registry)
	b.Cleanup(ctx.Reset)
	calls := make([]*core.CallExpr, len(samples))
	results := make([]core.Value, len(samples))
	for i, sample := range samples {
		args := []core.Argument{
			{
				Value: &core.ConstExpr{V: sample.value},
			},
		}
		calls[i] = &core.CallExpr{
			Node: core.NewNode(transformToStringBenchmarkPosition{}),
			Name: fn.Name(),
			Args: args,
			Plan: fn.PrepareCall(args),
		}
		require.NotNil(b, calls[i].Plan)
		result, err := calls[i].Eval(ctx)
		require.NoError(b, err)
		results[i] = result
		requireTransformToStringResult(b, sample, result)
	}
	if len(warmups) != 0 {
		join, found := registry.Get("join")
		require.True(b, found)
		for _, sample := range warmups {
			args := []core.Argument{
				{
					Value: &core.ConstExpr{V: sample.value},
				},
				{
					Value: &core.ConstExpr{V: core.StringValue{V: ":"}},
				},
			}
			call := &core.CallExpr{
				Node: core.NewNode(transformToStringBenchmarkPosition{}),
				Name: join.Name(),
				Args: args,
				Plan: join.PrepareCall(args),
			}
			require.NotNil(b, call.Plan)
			result, err := call.Eval(ctx)
			require.NoError(b, err)
			parts, valid := sample.input.([]string)
			require.True(b, valid, "join warmup requires string arrays")
			require.Equal(b, core.StringValue{V: strings.Join(parts, ":")}, result)
		}
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
		requireTransformToStringResult(b, sample, results[i])
	}
}

func requireTransformToStringResult(tb testing.TB, sample transformToStringSample, result core.Value) {
	tb.Helper()
	value, ok := result.(core.StringValue)
	require.True(tb, ok, "to_string must return a string")
	if sample.unordered {
		// These fixtures contain only JSON-compatible quoted strings. Object
		// key order follows map iteration and is not part of the contract.
		require.JSONEq(tb, sample.want, value.V)
		return
	}
	require.Equal(tb, sample.want, value.V)
}

func TestTransformToStringScenarios(t *testing.T) {
	scenarios := transformToStringScenarios()
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

type transformToStringBenchmarkPosition struct{}

func (transformToStringBenchmarkPosition) String() string { return "to-string-benchmark:1:1" }

type transformToStringSample struct {
	name      string
	value     core.Value
	input     any
	want      string
	unordered bool
}

type transformToStringWorkload struct {
	name    string
	samples []transformToStringSample
}

type transformToStringGroup struct {
	name    string
	samples func(int) []transformToStringSample
}

type transformToStringPattern struct {
	name  string
	words []string
}

func transformToStringPatterns() []transformToStringPattern {
	return []transformToStringPattern{
		{
			name:  "ascii",
			words: []string{"api", "request", "", "completed"},
		},
		{
			name:  "escaped_unicode",
			words: []string{"запрос", "界🙂", "line\nnext", `"quoted"\path`},
		},
	}
}

func transformToStringArraySamples(count int) []transformToStringSample {
	patterns := transformToStringPatterns()
	samples := make([]transformToStringSample, 0, len(patterns))
	for _, pattern := range patterns {
		values := make([]core.Value, count)
		input := make([]string, count)
		quoted := make([]string, count)
		for i := range values {
			word := pattern.words[i%len(pattern.words)]
			values[i] = core.StringValue{V: word}
			input[i] = word
			quoted[i] = strconv.Quote(word)
		}
		samples = append(samples, transformToStringSample{
			name:  pattern.name,
			value: core.ArrayValue{V: values},
			input: input,
			want:  "[" + strings.Join(quoted, ", ") + "]",
		})
	}
	return samples
}

func transformToStringObjectSamples(count int) []transformToStringSample {
	patterns := transformToStringPatterns()
	samples := make([]transformToStringSample, 0, len(patterns))
	for _, pattern := range patterns {
		values := make(map[string]core.Value, count)
		input := make(map[string]string, count)
		quoted := make([]string, count)
		for i := range count {
			key := "field_" + strconv.Itoa(i)
			word := pattern.words[i%len(pattern.words)]
			values[key] = core.StringValue{V: word}
			input[key] = word
			quoted[i] = strconv.Quote(key) + ": " + strconv.Quote(word)
		}
		samples = append(samples, transformToStringSample{
			name:      pattern.name,
			value:     core.ObjectValue{V: values},
			input:     input,
			want:      "{" + strings.Join(quoted, ", ") + "}",
			unordered: true,
		})
	}
	return samples
}

// A single field gives Do a stable formatted string without changing its
// shared result checker to interpret strings as unordered JSON objects.
func transformToStringWrappedArraySamples(count int) []transformToStringSample {
	samples := transformToStringArraySamples(count)
	for i, sample := range samples {
		samples[i] = transformToStringSample{
			name: sample.name,
			value: core.ObjectValue{V: map[string]core.Value{
				"items": sample.value,
			}},
			input: map[string]any{
				"items": sample.input,
			},
			want: `{"items": ` + sample.want + "}",
		}
	}
	return samples
}

func transformToStringRepeatedArray(sample transformToStringSample, count int) transformToStringSample {
	values := make([]core.Value, count)
	input := make([]any, count)
	parts := make([]string, count)
	for i := range values {
		values[i] = sample.value
		input[i] = sample.input
		parts[i] = sample.want
	}
	return transformToStringSample{
		name:  sample.name,
		value: core.ArrayValue{V: values},
		input: input,
		want:  "[" + strings.Join(parts, ", ") + "]",
	}
}

func transformToStringWorkloads(groups []transformToStringGroup) []transformToStringWorkload {
	counts := []int{0, 1, 2, 4, 16, 64, 256}
	var workloads []transformToStringWorkload
	for _, group := range groups {
		for _, count := range counts {
			workloads = append(workloads, transformToStringWorkload{
				name:    group.name + "/" + strconv.Itoa(count),
				samples: group.samples(count),
			})
		}
	}

	// Repeated operands share immutable values in Prepared, while each native
	// array encountered during recursive formatting still needs its own call.
	repeated := transformToStringArraySamples(16)
	for i, sample := range repeated {
		repeated[i] = transformToStringRepeatedArray(sample, 4)
	}
	deep := transformToStringArraySamples(4)
	for i, sample := range deep {
		for range 3 {
			sample = transformToStringRepeatedArray(sample, 2)
		}
		deep[i] = sample
	}
	workloads = append(workloads,
		transformToStringWorkload{
			name:    "nested/repeated",
			samples: repeated,
		},
		transformToStringWorkload{
			name:    "nested/deep",
			samples: deep,
		},
	)
	return append(workloads, transformToStringScalarWorkloads()...)
}

func transformToStringScalarWorkloads() []transformToStringWorkload {
	patterns := transformToStringPatterns()
	stringSamples := make([]transformToStringSample, 0, len(patterns))
	for _, pattern := range patterns {
		word := pattern.words[0]
		stringSamples = append(stringSamples, transformToStringSample{
			name:  pattern.name,
			value: core.StringValue{V: word},
			input: word,
			want:  word,
		})
	}
	integers := []int64{42, -17}
	integerSamples := make([]transformToStringSample, 0, len(integers))
	for _, value := range integers {
		text := strconv.FormatInt(value, 10)
		integerSamples = append(integerSamples, transformToStringSample{
			name:  text,
			value: core.IntegerValue{V: value},
			input: value,
			want:  text,
		})
	}
	nullSamples := []transformToStringSample{
		{
			name:  "null",
			value: core.NullValue{},
			input: nil,
			want:  "",
		},
	}
	return []transformToStringWorkload{
		{
			name:    "scalar/string",
			samples: stringSamples,
		},
		{
			name:    "scalar/integer",
			samples: integerSamples,
		},
		{
			name:    "scalar/null",
			samples: nullSamples,
		},
	}
}

func transformToStringScenarios() []transformCallScenario {
	groups := []transformToStringGroup{
		{
			name:    "array",
			samples: transformToStringArraySamples,
		},
		{
			name:    "wrapped_array",
			samples: transformToStringWrappedArraySamples,
		},
	}
	workloads := transformToStringWorkloads(groups)
	scenarios := make([]transformCallScenario, 0, len(workloads))
	for _, workload := range workloads {
		inputs := make([]transformCallCase, 0, len(workload.samples))
		for _, sample := range workload.samples {
			inputs = append(inputs, transformEventCase(sample.name,
				map[string]any{
					"value": sample.input,
					"out":   "stale",
					"keep":  true,
				},
				map[string]any{
					"value": sample.input,
					"out":   sample.want,
					"keep":  true,
				},
			))
		}
		scenarios = append(scenarios, transformCallScenario{
			name: workload.name,
			source: `
				.out = to_string(.value)
			`,
			inputs: inputs,
		})
	}
	return scenarios
}

func transformToStringAfterLargeInputs(count int, warmup bool) []transformCallCase {
	samples := transformToStringArraySamples(count)
	inputs := make([]transformCallCase, 0, len(samples))
	for _, sample := range samples {
		want := sample.want
		if warmup {
			want = strings.Join(sample.input.([]string), ":")
		}
		inputs = append(inputs, transformEventCase(sample.name,
			map[string]any{
				"value":  sample.input,
				"warmup": warmup,
				"out":    "stale",
			},
			map[string]any{
				"value":  sample.input,
				"warmup": warmup,
				"out":    want,
			},
		))
	}
	return inputs
}
