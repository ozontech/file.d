package transformbench

import (
	"fmt"
	"maps"
)

func transformCollectionScenarios() []transformCallScenario {
	// Repeating groups contain two selected amounts (10 and 7), one disabled
	// amount (1000), and one below the event's threshold (3). Expected totals
	// follow the declared composition, independently of the transform program.
	var inputs []transformCallCase
	for _, count := range []int{4, 64, 256} {
		items := make([]map[string]any, 0, count)
		for range count / 4 {
			items = append(items,
				map[string]any{"enabled": true, "amount": 10},
				map[string]any{"enabled": false, "amount": 1000},
				map[string]any{"enabled": true, "amount": 3},
				map[string]any{"enabled": true, "amount": 7},
			)
		}
		input := transformFieldEvent(false)
		input["items"], input["minimum"] = items, 5
		input["summary"] = map[string]any{"count": -1, "total": -1, "stale": true}
		want := maps.Clone(input)
		want["summary"] = map[string]any{"count": count / 2, "total": count / 4 * 17}
		inputs = append(inputs, transformEventCase(fmt.Sprint(count), input, want))
	}
	empty := transformCallCase{
		name:  "empty",
		input: `{"items":[],"minimum":5,"summary":{"count":99,"total":99},"keep":true}`,
		want:  `{"items":[],"minimum":5,"summary":{"count":0,"total":0},"keep":true}`,
	}
	base := transformCallScenario{
		// Modify cannot aggregate a variable-length collection or write the
		// typed summary. Locals and loop state must start fresh for every event.
		source: `
			count = 0
			total = 0
			if .items != null {
				for _, item in .items {
					if item.enabled && item.amount != null && item.amount >= .minimum {
						count = count + 1
						total = total + item.amount
					}
				}
			}
			.summary = {count: count, total: total}
		`,
		cases: []transformCallCase{
			empty,
			{
				name:  "missing_items",
				input: `{"minimum":5,"summary":"old","keep":[1,null]}`,
				want:  `{"minimum":5,"summary":{"count":0,"total":0},"keep":[1,null]}`,
			},
			{
				name:  "null_items",
				input: `{"items":null,"minimum":5}`,
				want:  `{"items":null,"minimum":5,"summary":{"count":0,"total":0}}`,
			},
			{
				name:  "short_circuit_and_missing_amount",
				input: `{"items":[{"enabled":false,"amount":"ignored"},{"enabled":true},{"enabled":true,"amount":null}],"minimum":5}`,
				want:  `{"items":[{"enabled":false,"amount":"ignored"},{"enabled":true},{"enabled":true,"amount":null}],"minimum":5,"summary":{"count":0,"total":0}}`,
			},
			{
				name:  "inclusive_threshold_and_float",
				input: `{"items":[{"enabled":true,"amount":-2},{"enabled":true,"amount":1.5},{"enabled":true,"amount":-3}],"minimum":-2}`,
				want:  `{"items":[{"enabled":true,"amount":-2},{"enabled":true,"amount":1.5},{"enabled":true,"amount":-3}],"minimum":-2,"summary":{"count":2,"total":-0.5}}`,
			},
		},
	}
	var scenarios []transformCallScenario
	for _, input := range inputs {
		scenario := base
		scenario.name = "collections/filter_sum/" + input.name
		scenario.inputs = []transformCallCase{input}
		scenarios = append(scenarios, scenario)
	}
	base.name = "collections/filter_sum/mixed"
	// A four-event cycle: 4 items, 64 items, 256 items, then an empty array.
	// Each contributes 1/4 of the events, not 1/4 of the processed items.
	inputs = append(inputs, empty)
	base.inputs = inputs
	return append(scenarios, base)
}
