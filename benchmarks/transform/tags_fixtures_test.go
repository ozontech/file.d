package transformbench

import (
	"fmt"
	"maps"

	"github.com/ozontech/file.d/plugin/action/modify"
)

type transformTagSample struct {
	name    string
	tags    [][2]string
	matched bool
	want    string
}

func transformTagScenarios() []transformCallScenario {
	var scenarios []transformCallScenario
	for _, limit := range []int{4, 16, 64} {
		// The paired domain is missing/null tags or at most limit string
		// pairs. The transform program itself has no artificial length limit.
		var actions []transformCallAction
		for i := range limit {
			actions = append(actions, transformCallAction{
				config: &modify.Config{"miniapp": fmt.Sprintf("${data.tags.%d.1}", i)},
				doIf: map[string]any{
					"op": "equal", "field": fmt.Sprintf("data.tags.%d.0", i),
					"values": "miniapp", "case_sensitive": true,
				},
			})
		}
		first, last := transformTagPairs(limit), transformTagPairs(limit)
		first[0] = [2]string{"miniapp", "checkout"}
		last[limit-1] = [2]string{"miniapp", "feed"}
		var inputs []transformCallCase
		samples := []transformTagSample{
			{
				name:    "full/first",
				tags:    first,
				matched: true,
				want:    "checkout",
			},
			{
				name:    "full/last",
				tags:    last,
				matched: true,
				want:    "feed",
			},
			{
				name: "full/no_match",
				tags: transformTagPairs(limit),
			},
			{
				name: "short/last",
				tags: [][2]string{
					{"kind", "web"},
					{"miniapp", "карта"},
				},
				matched: true,
				want:    "карта",
			},
			{
				name: "empty",
				tags: [][2]string{},
			},
			{
				name: "repeated",
				tags: [][2]string{
					{"miniapp", "first"},
					{"env", "prod"},
					{"miniapp", ""},
				},
				matched: true,
				want:    "",
			},
		}
		for _, sample := range samples {
			input := transformFieldEvent(false)
			input["data"] = map[string]any{"tags": sample.tags, "enabled": true}
			want := maps.Clone(input)
			if sample.matched {
				want["miniapp"] = sample.want
			}
			inputs = append(inputs, transformEventCase(sample.name, input, want))
		}
		base := transformCallScenario{
			// Keep the user's original program, including last-match-wins.
			source: `
				if .data.tags != null {
					for i in .data.tags {
						if .data.tags[i][0] == "miniapp" {
							.miniapp = .data.tags[i][1]
						}
					}
				}
			`,
			actions: actions,
			cases: []transformCallCase{
				{
					name:  "missing_data",
					input: `{"keep":true}`,
					want:  `{"keep":true}`,
				},
				{
					name:  "missing_tags",
					input: `{"data":{"keep":42},"miniapp":"keep"}`,
					want:  `{"data":{"keep":42},"miniapp":"keep"}`,
				},
				{
					name:  "null_tags",
					input: `{"data":{"tags":null},"miniapp":null}`,
					want:  `{"data":{"tags":null},"miniapp":null}`,
				},
				{
					name:  "empty_tags_preserve_type",
					input: `{"data":{"tags":[]},"miniapp":42}`,
					want:  `{"data":{"tags":[]},"miniapp":42}`,
				},
				{
					name:  "no_match_preserve_destination",
					input: `{"data":{"tags":[["env","prod"]]},"miniapp":{"keep":true}}`,
					want:  `{"data":{"tags":[["env","prod"]]},"miniapp":{"keep":true}}`,
				},
				{
					name:  "case_sensitive",
					input: `{"data":{"tags":[["MiniApp","ignored"]]},"miniapp":false}`,
					want:  `{"data":{"tags":[["MiniApp","ignored"]]},"miniapp":false}`,
				},
				{
					name:  "overwrite_destination",
					input: `{"data":{"tags":[["miniapp","checkout"]]},"miniapp":"old"}`,
					want:  `{"data":{"tags":[["miniapp","checkout"]]},"miniapp":"checkout"}`,
				},
				{
					name:  "last_match_wins",
					input: `{"data":{"tags":[["miniapp","first"],["miniapp","last"]]},"miniapp":"old"}`,
					want:  `{"data":{"tags":[["miniapp","first"],["miniapp","last"]]},"miniapp":"last"}`,
				},
				{
					name:  "last_match_is_empty",
					input: `{"data":{"tags":[["miniapp","first"],["miniapp",""]]},"miniapp":"old"}`,
					want:  `{"data":{"tags":[["miniapp","first"],["miniapp",""]]},"miniapp":""}`,
				},
			},
		}
		// The first four samples are homogeneous profiles. Empty and repeated
		// matches are measured as part of the six-event mixed corpus.
		for _, input := range inputs[:4] {
			scenario := base
			scenario.name = fmt.Sprintf("chains/tags/%d/%s", limit, input.name)
			scenario.inputs = []transformCallCase{input}
			scenarios = append(scenarios, scenario)
		}
		base.name = fmt.Sprintf("chains/tags/%d/mixed", limit)
		// Each of the six samples has weight 1/6 in a complete cycle.
		base.inputs = inputs
		scenarios = append(scenarios, base)
	}
	return scenarios
}

func transformTagPairs(count int) [][2]string {
	tags := make([][2]string, count)
	for i := range tags {
		tags[i] = [2]string{fmt.Sprintf("tag_%02d", i), fmt.Sprintf("value_%02d", i)}
	}
	return tags
}
