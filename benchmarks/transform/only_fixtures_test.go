package transformbench

import (
	"fmt"
	"maps"
	"strings"
)

func transformOnlyScenarios() []transformCallScenario {
	scenarios := transformDynamicScenarios()
	scenarios = append(scenarios, transformCollectionScenarios()...)
	scenarios = append(scenarios, transformTypedTagScenarios()...)
	return scenarios
}

func transformDynamicScenarios() []transformCallScenario {
	// The cutset and both slice bounds come from the event. Modify filter
	// arguments are configuration constants, so it cannot implement this domain.
	var inputs []transformCallCase
	for _, size := range []int{128, 4096} {
		input := transformFieldEvent(false)
		input["source"] = "  [[HEAD" + strings.Repeat("x", size-16) + "TAIL]]  "
		input["cutset"], input["start"], input["end"] = " []", 0, 4
		input["out"] = "old"
		want := maps.Clone(input)
		want["out"] = "HEAD"
		inputs = append(inputs, transformEventCase(fmt.Sprintf("%dB", size), input, want))
	}
	tail := transformCallCase{
		name:  "negative_start_null_end",
		input: `{"source":"..{{otherTAIL}}..","cutset":".{}","start":-4,"end":null,"out":"old","keep":true}`,
		want:  `{"source":"..{{otherTAIL}}..","cutset":".{}","start":-4,"end":null,"out":"TAIL","keep":true}`,
	}
	unicode := transformCallCase{
		name:  "unicode",
		input: `{"source":"яя[привет]яя","cutset":"я[]","start":1,"end":-1,"keep":{"count":7}}`,
		want:  `{"source":"яя[привет]яя","cutset":"я[]","start":1,"end":-1,"out":"риве","keep":{"count":7}}`,
	}
	base := transformCallScenario{
		source: `
			.out = slice(trim(.source, .cutset), .start, end: .end)
		`,
		cases: []transformCallCase{
			tail,
			unicode,
			{
				name:  "empty",
				input: `{"source":"","cutset":" []","start":0,"end":4,"out":"old"}`,
				want:  `{"source":"","cutset":" []","start":0,"end":4,"out":""}`,
			},
			{
				name:  "only_cutset",
				input: `{"source":" [[]] ","cutset":" []","start":0,"end":null}`,
				want:  `{"source":" [[]] ","cutset":" []","start":0,"end":null,"out":""}`,
			},
			{
				name:  "clamped_bounds",
				input: `{"source":"api","cutset":"[]","start":-100,"end":100}`,
				want:  `{"source":"api","cutset":"[]","start":-100,"end":100,"out":"api"}`,
			},
			{
				name:  "reversed_bounds",
				input: `{"source":"api","cutset":"","start":2,"end":1}`,
				want:  `{"source":"api","cutset":"","start":2,"end":1,"out":""}`,
			},
			{
				name:  "missing_end",
				input: `{"source":"abc","cutset":"","start":1}`,
				want:  `{"source":"abc","cutset":"","start":1,"out":"bc"}`,
			},
		},
	}
	profiles := []transformCallProfile{
		{
			name:   "128B",
			inputs: inputs[:1],
		},
		{
			name:   "4096B",
			inputs: inputs[1:],
		},
		// One of each, in order: 128 B, 4096 B, negative start/null end,
		// and Unicode/negative end. Every event has weight 1/4.
		{
			name:   "mixed",
			inputs: []transformCallCase{inputs[0], inputs[1], tail, unicode},
		},
	}
	var scenarios []transformCallScenario
	for _, profile := range profiles {
		scenario := base
		scenario.name = "dynamic/trim_slice/" + profile.name
		scenario.inputs = profile.inputs
		scenarios = append(scenarios, scenario)
	}
	return scenarios
}
