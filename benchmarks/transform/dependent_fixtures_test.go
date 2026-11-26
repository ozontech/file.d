package transformbench

import (
	"fmt"
	"maps"
	"strings"

	"github.com/ozontech/file.d/plugin/action/modify"
)

type transformDependentSample struct {
	name    string
	size    int
	service string
	route   string
}

func transformDependentScenarios() []transformCallScenario {
	// The log has one bracketed service token followed by a message without
	// brackets. Service is always extracted; route changes only for "api".
	// A single modify cannot apply its do_if to just one of its assignments.
	var inputs []transformCallCase
	samples := []transformDependentSample{
		{
			name:    "short",
			size:    128,
			service: "api",
			route:   "frontend",
		},
		{
			name:    "long",
			size:    4096,
			service: "api",
			route:   "frontend",
		},
		{
			name:    "no_match",
			size:    512,
			service: "worker",
			route:   "keep",
		},
	}
	for _, sample := range samples {
		prefix := "[" + sample.service + "] "
		input := transformFieldEvent(false)
		input["log"] = prefix + strings.Repeat("x", sample.size-len(prefix))
		// Deliberately stale fields: the condition must read service after
		// extraction, including a no-match event after matching events.
		input["service"], input["route"] = "api", "keep"
		want := maps.Clone(input)
		want["service"], want["route"] = sample.service, sample.route
		inputs = append(inputs, transformEventCase(sample.name, input, want))
	}
	base := transformCallScenario{
		source: `
			.service = between(.log, "[", "]")
			if .service == "api" {
				.route = "frontend"
			}
		`,
		actions: []transformCallAction{
			{
				config: &modify.Config{
					"service": `${log|trim_to("left","[")|trim_to("right","]")|trim("all","[]")}`,
				},
			},
			{
				config: &modify.Config{"route": "frontend"},
				doIf: map[string]any{
					"op": "equal", "field": "service", "values": "api", "case_sensitive": true,
				},
			},
		},
		cases: []transformCallCase{
			{
				name:  "condition_uses_extracted_service",
				input: `{"log":"[api] request","service":"worker","keep":true}`,
				want:  `{"log":"[api] request","service":"api","route":"frontend","keep":true}`,
			},
			{
				name:  "no_match_keeps_route_absent",
				input: `{"log":"[worker] request","service":"api"}`,
				want:  `{"log":"[worker] request","service":"worker"}`,
			},
			{
				name:  "no_match_preserves_route_type",
				input: `{"log":"[worker] request","route":42}`,
				want:  `{"log":"[worker] request","service":"worker","route":42}`,
			},
			{
				name:  "case_sensitive",
				input: `{"log":"[API] request","route":"keep"}`,
				want:  `{"log":"[API] request","service":"API","route":"keep"}`,
			},
			{
				name:  "empty_service",
				input: `{"log":"[] request","route":"keep"}`,
				want:  `{"log":"[] request","service":"","route":"keep"}`,
			},
			{
				name:  "unicode_service",
				input: `{"log":"[заказы] запрос","route":"keep"}`,
				want:  `{"log":"[заказы] запрос","service":"заказы","route":"keep"}`,
			},
		},
	}
	var scenarios []transformCallScenario
	profiles := []transformCallProfile{
		{
			name:   "128B",
			inputs: inputs[:1],
		},
		{
			name:   "4096B",
			inputs: inputs[1:2],
		},
		// 1/3 short match, 1/3 long match, 1/3 512-byte no-match.
		{
			name:   "mixed",
			inputs: inputs,
		},
	}
	for _, profile := range profiles {
		scenario := base
		scenario.name = fmt.Sprintf("chains/dependent/%s", profile.name)
		scenario.inputs = profile.inputs
		scenarios = append(scenarios, scenario)
	}
	return scenarios
}
