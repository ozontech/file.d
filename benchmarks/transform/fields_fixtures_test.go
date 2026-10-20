package transformbench

import (
	"fmt"
	"maps"
	"strings"

	"github.com/ozontech/file.d/plugin/action/modify"
	"github.com/ozontech/file.d/plugin/action/rename"
)

func transformFieldScenarios() []transformCallScenario {
	var scenarios []transformCallScenario
	// The original five-field assignments remain unchanged in the control set.
	for _, count := range []int{1, 20} {
		scenarios = append(scenarios,
			transformAssignScenario(count, false, false),
			transformAssignScenario(count, true, false))
	}
	scenarios = append(scenarios,
		transformAssignScenario(5, false, true),
		transformAssignScenario(5, true, true))
	for _, count := range []int{1, 5, 20} {
		scenarios = append(scenarios, transformCopyScenario(count))
	}
	scenarios = append(scenarios, transformCallScenario{
		name: "copy/nested",
		source: `
			.out.host = .data.host
			.out.zone = .data.meta.zone
		`,
		actions: []transformCallAction{{config: &modify.Config{
			"out.host": "${data.host}", "out.zone": "${data.meta.zone}",
		}}},
		inputs: []transformCallCase{{
			name:  "strings",
			input: `{"data":{"host":"api","meta":{"zone":"west","enabled":true}},"keep":[1,2]}`,
			want:  `{"data":{"host":"api","meta":{"zone":"west","enabled":true}},"keep":[1,2],"out":{"host":"api","zone":"west"}}`,
		}},
		cases: []transformCallCase{{
			name:  "overwrite_and_preserve",
			input: `{"data":{"host":"","meta":{"zone":"east"}},"out":{"host":"old","zone":"old","keep":42}}`,
			want:  `{"data":{"host":"","meta":{"zone":"east"}},"out":{"host":"","zone":"east","keep":42}}`,
		}},
	}, transformCallScenario{
		name: "compose/nested_fields",
		source: `
			.out = .service + "/" + .meta.zone + ":" + to_string(.status)
		`,
		actions: []transformCallAction{{config: &modify.Config{
			"out": "${service}/${meta.zone}:${status}",
		}}},
		inputs: []transformCallCase{{
			name:  "status",
			input: `{"service":"checkout","meta":{"zone":"west","tags":["api"]},"status":200}`,
			want:  `{"service":"checkout","meta":{"zone":"west","tags":["api"]},"status":200,"out":"checkout/west:200"}`,
		}},
		cases: []transformCallCase{{
			name:  "unicode_and_empty",
			input: `{"service":"заказ","meta":{"zone":""},"status":-1,"out":"old"}`,
			want:  `{"service":"заказ","meta":{"zone":""},"status":-1,"out":"заказ/:-1"}`,
		}},
	})
	for _, count := range []int{1, 5, 20} {
		for _, override := range []bool{false, true} {
			scenarios = append(scenarios, transformRenameScenario(count, override, false))
		}
	}
	return append(scenarios, transformRenameScenario(5, false, true))
}

// A large containing event adds 64 unrelated fields and a 4 KiB payload.
// These are preserved in every expected result, not transformed or discarded.
func transformFieldEvent(large bool) map[string]any {
	event := map[string]any{"keep": map[string]any{"enabled": true, "tags": []string{"api", "edge"}}}
	if large {
		for i := range 64 {
			event[fmt.Sprintf("extra_%02d", i)] = fmt.Sprintf("value_%02d", i)
		}
		event["payload"] = strings.Repeat("x", 4096)
	}
	return event
}

func transformAssignScenario(count int, existing, large bool) transformCallScenario {
	mode := "new"
	if existing {
		mode = "existing"
	}
	name := fmt.Sprintf("assign/%s/%d", mode, count)
	if large {
		name += "/large_event"
	}
	input := transformFieldEvent(large)
	want := maps.Clone(input)
	config := make(modify.Config, count)
	var source strings.Builder
	for i := range count {
		field, value := fmt.Sprintf("field_%02d", i), fmt.Sprintf("value_%02d", i)
		if existing {
			input[field] = "old"
		}
		want[field] = value
		config[field] = value
		fmt.Fprintf(&source, ".%s = %q\n", field, value)
	}
	return transformCallScenario{
		name: name, source: source.String(),
		actions: []transformCallAction{{config: &config}},
		inputs:  []transformCallCase{transformEventCase("default", input, want)},
	}
}

// Copy inputs are strings: modify stringifies values, whereas transform can
// preserve other JSON types. Numeric conversion is covered explicitly above.
func transformCopyScenario(count int) transformCallScenario {
	input := transformFieldEvent(false)
	want := maps.Clone(input)
	config := make(modify.Config, count)
	var source strings.Builder
	for i := range count {
		from, to := fmt.Sprintf("old_%02d", i), fmt.Sprintf("new_%02d", i)
		value := fmt.Sprintf("value_%02d", i)
		input[from] = value
		want[from], want[to] = value, value
		config[to] = "${" + from + "}"
		fmt.Fprintf(&source, ".%s = .%s\n", to, from)
	}
	return transformCallScenario{
		name: fmt.Sprintf("copy/%d", count), source: source.String(),
		actions: []transformCallAction{{config: &config}},
		inputs:  []transformCallCase{transformEventCase("default", input, want)},
	}
}

// The language treats missing and null identically in comparisons. For these
// guarded rename programs, sources are absent or non-null; preserve mode also
// requires destinations to be absent or non-null. Nested nulls remain valid.
func transformRenameScenario(count int, override, large bool) transformCallScenario {
	mode := "preserve"
	if override {
		mode = "override"
	}
	name := fmt.Sprintf("rename/%s/%d", mode, count)
	if large {
		name += "/large_event"
	}
	input := transformFieldEvent(large)
	want := maps.Clone(input)
	config := rename.Config{"override", fmt.Sprint(override)}
	values := []any{"api", int64(42), false, []any{1, "x", nil}, map[string]any{"zone": "west", "value": nil}}
	var source strings.Builder
	for i := range count {
		from, to := fmt.Sprintf("old_%02d", i), fmt.Sprintf("new_%02d", i)
		value := values[i%len(values)]
		input[from], want[to] = value, value
		config.Append(from, to)
		if override {
			input[to] = "old"
			fmt.Fprintf(&source, "if .%s != null {\n", from)
		} else {
			fmt.Fprintf(&source, "if .%s != null && .%s == null {\n", from, to)
		}
		fmt.Fprintf(&source, "  .%s = .%s\n  del .%s\n}\n", to, from, from)
	}
	missing, missingWant := maps.Clone(input), maps.Clone(want)
	delete(missing, "old_00")
	if override {
		missingWant["new_00"] = "old"
	} else {
		delete(missingWant, "new_00")
	}
	collision, collisionWant := maps.Clone(input), maps.Clone(want)
	collision["new_00"] = "keep"
	if !override {
		collisionWant["new_00"] = "keep"
		collisionWant["old_00"] = input["old_00"]
	}
	return transformCallScenario{
		name: name, source: source.String(),
		actions: []transformCallAction{{config: &config}},
		inputs:  []transformCallCase{transformEventCase("typed_fields", input, want)},
		cases: []transformCallCase{
			transformEventCase("missing_source", missing, missingWant),
			transformEventCase("existing_destination", collision, collisionWant),
		},
	}
}
