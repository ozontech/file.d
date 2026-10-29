package transformbench

import (
	"fmt"
	"maps"
)

type transformTypedTagSample struct {
	name    string
	count   int
	value   any
	noMatch bool
}

func transformTypedTagScenarios() []transformCallScenario {
	// Extend the original tag program to JSON values and arrays beyond the
	// paired limit of 64. Modify stringifies values and a fixed action chain
	// cannot handle arbitrary lengths, so no comparator is configured here.
	object := map[string]any{
		"name":    "карта",
		"enabled": true,
		"ports":   []int{443, 8443},
	}
	samples := []transformTypedTagSample{
		{
			name:  "4/integer",
			count: 4,
			value: 42,
		},
		{
			name:  "256/integer",
			count: 256,
			value: 42,
		},
		{
			name:  "4/object",
			count: 4,
			value: object,
		},
		{
			name:  "256/object",
			count: 256,
			value: object,
		},
		{
			name:  "4/bool",
			count: 4,
			value: false,
		},
		{
			name:  "4/float",
			count: 4,
			value: 1.5,
		},
		{
			name:  "4/null",
			count: 4,
			value: nil,
		},
		{
			name:  "256/array",
			count: 256,
			value: []any{"api", 7, nil},
		},
		{
			name:    "256/no_match",
			count:   256,
			noMatch: true,
		},
		{
			name:  "empty",
			count: 0,
		},
	}
	inputs := make([]transformCallCase, 0, len(samples))
	for _, sample := range samples {
		tags := make([][2]any, sample.count)
		for i := range tags {
			tags[i] = [2]any{fmt.Sprintf("tag_%02d", i), i}
		}
		input := transformFieldEvent(false)
		input["data"] = map[string]any{"tags": tags, "enabled": true}
		input["miniapp"] = map[string]any{"keep": true}
		want := maps.Clone(input)
		if len(tags) != 0 && !sample.noMatch {
			// Every matched input has two matches. The last value, including
			// false or null, must replace the earlier string and its JSON type.
			tags[0] = [2]any{"miniapp", "earlier"}
			tags[len(tags)-1] = [2]any{"miniapp", sample.value}
			want["miniapp"] = sample.value
		}
		inputs = append(inputs, transformEventCase(sample.name, input, want))
	}
	base := transformCallScenario{
		source: `
			if .data.tags != null {
				for i in .data.tags {
					if .data.tags[i][0] == "miniapp" {
						.miniapp = .data.tags[i][1]
					}
				}
			}
		`,
		cases: []transformCallCase{
			{
				name:  "missing_data",
				input: `{"miniapp":{"keep":true},"keep":[1,null]}`,
				want:  `{"miniapp":{"keep":true},"keep":[1,null]}`,
			},
			{
				name:  "missing_tags",
				input: `{"data":{"keep":true}}`,
				want:  `{"data":{"keep":true}}`,
			},
			{
				name:  "null_tags",
				input: `{"data":{"tags":null},"miniapp":[1,null]}`,
				want:  `{"data":{"tags":null},"miniapp":[1,null]}`,
			},
			{
				name:  "no_match_keeps_destination_absent",
				input: `{"data":{"tags":[["other",{"enabled":false}]]}}`,
				want:  `{"data":{"tags":[["other",{"enabled":false}]]}}`,
			},
			{
				name:  "nested_null_and_empty_containers",
				input: `{"data":{"tags":[["miniapp",{"null":null,"array":[],"object":{}}]]}}`,
				want:  `{"data":{"tags":[["miniapp",{"null":null,"array":[],"object":{}}]]},"miniapp":{"null":null,"array":[],"object":{}}}`,
			},
			{
				name:  "last_empty_string",
				input: `{"data":{"tags":[["miniapp",42],["miniapp",""]]},"miniapp":false}`,
				want:  `{"data":{"tags":[["miniapp",42],["miniapp",""]]},"miniapp":""}`,
			},
		},
	}
	var scenarios []transformCallScenario
	for _, input := range inputs[:4] {
		scenario := base
		scenario.name = "tags/typed/" + input.name
		scenario.inputs = []transformCallCase{input}
		scenarios = append(scenarios, scenario)
	}
	base.name = "tags/typed/mixed"
	// Ten events in sample order, each with weight 1/10. Alongside the four
	// homogeneous profiles, measure bool/float/null/array, no match, and empty.
	base.inputs = inputs
	return append(scenarios, base)
}
