package transformbench

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkTransformRuntime measures the runtime reuse workloads separately
// from the paired report and its GROUP/CASE selection.
func BenchmarkTransformRuntime(b *testing.B) {
	for _, scenario := range transformRuntimeScenarios() {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

func TestTransformRuntimeScenarios(t *testing.T) {
	for _, scenario := range transformRuntimeScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

func transformRuntimeScenarios() []transformCallScenario {
	type nativeSample struct {
		name    string
		source  string
		number  int
		enabled bool
	}
	samples := []nativeSample{
		{
			name:    "plain",
			source:  "runtime event",
			number:  42,
			enabled: true,
		},
		{
			name:    "escaped_unicode",
			source:  "line \"quoted\"\\path\n<>&界🙂\u2028",
			number:  -7,
			enabled: false,
		},
	}
	var objectInputs, arrayInputs []transformCallCase
	for _, sample := range samples {
		input := map[string]any{
			"source":  sample.source,
			"number":  sample.number,
			"enabled": sample.enabled,
			"out":     map[string]any{"stale": true},
			"keep":    []any{1, nil, "untouched"},
		}
		// Expected events follow the fixture data, without running transform.
		objectWant := maps.Clone(input)
		objectWant["out"] = map[string]any{
			"text":            sample.source,
			"number":          sample.number,
			"enabled":         sample.enabled,
			"empty":           "",
			"missing":         nil,
			"text_copy":       sample.source,
			"number_next":     sample.number + 1,
			"number_previous": sample.number - 1,
			"nested": map[string]any{
				"value":  sample.source,
				"count":  sample.number,
				"active": sample.enabled,
			},
			"values": []any{sample.source, sample.number, sample.enabled, nil},
			"nested_array": []any{
				map[string]any{
					"value": sample.source,
					"count": sample.number,
				},
				map[string]any{
					"value": sample.source,
					"count": sample.number + 1,
				},
			},
			"nested_object": map[string]any{
				"left":  map[string]any{"value": sample.source},
				"right": map[string]any{"number": sample.number},
			},
			"empty_array":  []any{},
			"empty_object": map[string]any{},
			"label":        "runtime",
			"tail":         sample.source,
		}
		objectInputs = append(objectInputs, transformEventCase(sample.name, input, objectWant))

		rows := make([]map[string]any, 16)
		for i := range rows {
			rows[i] = map[string]any{
				"index":   i,
				"text":    sample.source,
				"number":  sample.number,
				"enabled": sample.enabled,
				"missing": nil,
			}
		}
		arrayWant := maps.Clone(input)
		arrayWant["out"] = rows
		arrayInputs = append(arrayInputs, transformEventCase(sample.name, input, arrayWant))
	}

	scenarios := []transformCallScenario{
		{
			name:   "empty",
			source: "",
			inputs: []transformCallCase{
				{
					name:  "plain",
					input: `{"source":"unchanged","number":42}`,
					want:  `{"source":"unchanged","number":42}`,
				},
				{
					name:  "nested",
					input: `{"source":"界","keep":{"values":[false,null,7]}}`,
					want:  `{"source":"界","keep":{"values":[false,null,7]}}`,
				},
			},
		},
		{
			name: "locals",
			source: `
				.previous = final
				first = .number + 1
				second = first * 2
				final = second - 3
				.out = final
			`,
			inputs: []transformCallCase{
				{
					name:  "positive",
					input: `{"number":10,"keep":true}`,
					want:  `{"number":10,"keep":true,"previous":null,"out":19}`,
				},
				{
					name:  "negative",
					input: `{"number":-2,"out":"old","keep":false}`,
					want:  `{"number":-2,"out":-5,"keep":false,"previous":null}`,
				},
			},
		},
		{
			name: "metadata",
			source: `
				.previous = %third;
				%first = .source + "";
				%second = %first + "-done";
				%third = %second;
				.out = %third
			`,
			inputs: []transformCallCase{
				{
					name:  "plain",
					input: `{"source":"request","keep":7}`,
					want:  `{"source":"request","keep":7,"previous":null,"out":"request-done"}`,
				},
				{
					name:  "unicode",
					input: `{"source":"запрос","out":"old","keep":null}`,
					want:  `{"source":"запрос","out":"запрос-done","keep":null,"previous":null}`,
				},
			},
		},
		{
			name: "nested_calls",
			source: `
				.out = trim_right(
					trim_left(
						before(.source, "."),
						" "
					),
					"0123456789"
				)
			`,
			inputs: []transformCallCase{
				{
					name:  "plain",
					input: `{"source":"  api123.example","keep":true}`,
					want:  `{"source":"  api123.example","keep":true,"out":"api"}`,
				},
				{
					name:  "unicode",
					input: `{"source":"  узел42.domain","out":"old","keep":false}`,
					want:  `{"source":"  узел42.domain","out":"узел","keep":false}`,
				},
			},
		},
		{
			name: "native_object",
			// Materialize scalar locals so the native encoder handles these
			// leaves instead of serializing JSONNodeValue references.
			source: `
				text = .source + ""
				number = .number + 0
				enabled = .enabled == true
				.out = {
					text: text,
					number: number,
					enabled: enabled,
					empty: "",
					missing: null,
					text_copy: text,
					number_next: number + 1,
					number_previous: number - 1,
					nested: {value: text, count: number, active: enabled},
					values: [text, number, enabled, null],
					nested_array: [{value: text, count: number}, {value: text, count: number + 1}],
					nested_object: {left: {value: text}, right: {number: number}},
					empty_array: [],
					empty_object: {},
					label: "runtime",
					tail: text
				}
			`,
			inputs: objectInputs,
		},
		{
			name: "native_array",
			source: `
				text = .source + ""
				number = .number + 0
				enabled = .enabled == true
				.out = [
					{index: 0, text: text, number: number, enabled: enabled, missing: null},
					{index: 1, text: text, number: number, enabled: enabled, missing: null},
					{index: 2, text: text, number: number, enabled: enabled, missing: null},
					{index: 3, text: text, number: number, enabled: enabled, missing: null},
					{index: 4, text: text, number: number, enabled: enabled, missing: null},
					{index: 5, text: text, number: number, enabled: enabled, missing: null},
					{index: 6, text: text, number: number, enabled: enabled, missing: null},
					{index: 7, text: text, number: number, enabled: enabled, missing: null},
					{index: 8, text: text, number: number, enabled: enabled, missing: null},
					{index: 9, text: text, number: number, enabled: enabled, missing: null},
					{index: 10, text: text, number: number, enabled: enabled, missing: null},
					{index: 11, text: text, number: number, enabled: enabled, missing: null},
					{index: 12, text: text, number: number, enabled: enabled, missing: null},
					{index: 13, text: text, number: number, enabled: enabled, missing: null},
					{index: 14, text: text, number: number, enabled: enabled, missing: null},
					{index: 15, text: text, number: number, enabled: enabled, missing: null}
				]
			`,
			inputs: arrayInputs,
		},
	}
	return scenarios
}
