package transformbench

import (
	"fmt"
	"maps"
	"strings"

	"github.com/ozontech/file.d/plugin/action/modify"
)

type transformStringOperation struct {
	name        string
	source      string
	filter      string
	input       string
	want        string
	missProfile string
	missWant    string
	cases       []transformCallCase
}

func transformStringScenarios() []transformCallScenario {
	var scenarios []transformCallScenario
	for _, size := range []int{128, 4096} {
		payload := strings.Repeat("x", size-8)
		trimInput := "  [[" + payload + "]]  "
		delimited := "pre|" + payload + "|end"
		cutInput := "HEAD" + payload + "TAIL"
		regexPrefix := "id=42 id=7 id=99 "
		regexInput := regexPrefix + strings.Repeat("x", size-len(regexPrefix))
		operations := []transformStringOperation{
			{
				name:   "trim/all",
				source: `.out = trim(.source, " []")`,
				filter: `trim("all"," []")`,
				input:  trimInput,
				want:   payload,
				cases: []transformCallCase{
					transformStringCase("unicode", " [[привет]] ", "привет"),
					transformStringCase("only_cutset", " [[]] ", ""),
				},
			},
			{
				name:   "trim/left",
				source: `.out = trim_left(.source, " []")`,
				filter: `trim("left"," []")`,
				input:  trimInput,
				want:   payload + "]]  ",
				cases: []transformCallCase{
					transformStringCase("unicode", " [[привет]] ", "привет]] "),
				},
			},
			{
				name:   "trim/right",
				source: `.out = trim_right(.source, " []")`,
				filter: `trim("right"," []")`,
				input:  trimInput,
				want:   "  [[" + payload,
				cases: []transformCallCase{
					transformStringCase("unicode", " [[привет]] ", " [[привет"),
				},
			},
			{
				name:        "trim_to/all",
				source:      `.out = trim_to(.source, "|")`,
				filter:      `trim_to("all","|")`,
				input:       delimited,
				want:        "|" + payload + "|",
				missProfile: "no_delimiter",
				missWant:    strings.Repeat("x", 128),
				cases: []transformCallCase{
					transformStringCase("one_delimiter", "left|right", "|"),
					transformStringCase("repeated_delimiter", "pre|a|b|end", "|a|b|"),
					transformStringCase("unicode", "до|текст|после", "|текст|"),
				},
			},
			{
				name:        "trim_to/left",
				source:      `.out = trim_to_left(.source, "|")`,
				filter:      `trim_to("left","|")`,
				input:       delimited,
				want:        "|" + payload + "|end",
				missProfile: "no_delimiter",
				missWant:    strings.Repeat("x", 128),
				cases: []transformCallCase{
					transformStringCase("one_delimiter", "left|right", "|right"),
					transformStringCase("repeated_delimiter", "pre|a|b|end", "|a|b|end"),
				},
			},
			{
				name:        "trim_to/right",
				source:      `.out = trim_to_right(.source, "|")`,
				filter:      `trim_to("right","|")`,
				input:       delimited,
				want:        "pre|" + payload + "|",
				missProfile: "no_delimiter",
				missWant:    strings.Repeat("x", 128),
				cases: []transformCallCase{
					transformStringCase("one_delimiter", "left|right", "left|"),
					transformStringCase("repeated_delimiter", "pre|a|b|end", "pre|a|b|"),
				},
			},
			// modify cut counts bytes and transform slice counts runes.
			// Restrict these paired workloads to ASCII, including short inputs.
			{
				name:   "cut/first",
				source: `.out = slice(.source, 0, end: 4)`,
				filter: `cut("first",4)`,
				input:  cutInput,
				want:   "HEAD",
				cases: []transformCallCase{
					transformStringCase("short", "abc", "abc"),
					transformStringCase("exact_length", "abcd", "abcd"),
				},
			},
			{
				name:   "cut/last",
				source: `.out = slice(.source, -4)`,
				filter: `cut("last",4)`,
				input:  cutInput,
				want:   "TAIL",
				cases: []transformCallCase{
					transformStringCase("short", "abc", "abc"),
					transformStringCase("exact_length", "abcd", "abcd"),
				},
			},
			{
				name: "regex/first_group",
				source: `
					match = parse_regex(.source, r'id=(?P<id>[0-9]+)')
					if match != null {
						.out = match.id
					} else {
						.out = ""
					}
				`,
				filter:      `re("id=(?P<id>[0-9]+)",1,[1],"",true)`,
				input:       regexInput,
				want:        "42",
				missProfile: "no_match",
				cases: []transformCallCase{
					transformStringCase("one_match", "до id=123 после", "123"),
					transformStringCase("empty_group_is_not_a_match", "id=", ""),
				},
			},
			{
				name: "regex/all_groups",
				source: `
					.out = join(
						parse_regex_all(.source, r'id=([0-9]+)', group: 1, limit: 2),
						","
					)
				`,
				filter:      `re("id=([0-9]+)",2,[1],",",true)`,
				input:       regexInput,
				want:        "42,7",
				missProfile: "no_match",
				cases: []transformCallCase{
					transformStringCase("one_match", "до id=123 после", "123"),
					transformStringCase("empty_group_is_not_a_match", "id=", ""),
					transformStringCase("leading_zeroes", "id=01 id=002 id=3", "01,002"),
				},
			},
		}
		for i := range operations {
			op := &operations[i]
			actions := []transformCallAction{
				{
					config: &modify.Config{"out": "${source|" + op.filter + "}"},
				},
			}
			scenarios = append(scenarios, transformCallScenario{
				name:    fmt.Sprintf("strings/%s/%dB", op.name, size),
				source:  op.source,
				actions: actions,
				inputs:  []transformCallCase{transformStringCase("default", op.input, op.want)},
				cases:   append(op.cases, transformStringCase("empty", "", "")),
			})
			// Misses are a separate homogeneous workload at one size, avoiding
			// a full product of lengths and match positions.
			if size == 128 && op.missProfile != "" {
				scenarios = append(scenarios, transformCallScenario{
					name:    fmt.Sprintf("strings/%s/%s/128B", op.name, op.missProfile),
					source:  op.source,
					actions: actions,
					inputs:  []transformCallCase{transformStringCase("miss", strings.Repeat("x", 128), op.missWant)},
				})
			}
		}
	}
	return scenarios
}

func transformStringCase(name, source, out string) transformCallCase {
	input := transformFieldEvent(false)
	input["source"] = source
	input["out"] = "old"
	want := maps.Clone(input)
	want["out"] = out
	return transformEventCase(name, input, want)
}
