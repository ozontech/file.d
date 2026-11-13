package transformbench

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkTransformProductionNormalization measures the first transform
// program from the production profile configuration on synthetic decoded events.
// It excludes the surrounding decoders, flatten actions, pipeline I/O and setup.
func BenchmarkTransformProductionNormalization(b *testing.B) {
	scenarios := transformProductionNormalizationScenarios(b)
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

func TestTransformProductionNormalizationScenarios(t *testing.T) {
	scenarios := transformProductionNormalizationScenarios(t)
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

func transformProductionNormalizationScenarios(tb testing.TB) []transformCallScenario {
	tb.Helper()
	base := transformCallScenario{
		name: "mixed",
		source: `
			# Capture field presence once: a missing field and explicit null differ.
			fields = .;
			present = {}
			for _, key in ["message", "service", "host", "ci_job_id", "zone", "@lt", "event_message"] {
			  present[key] = lookup(key, fields, default: true) == lookup(key, fields, default: false)
			}

			# Keep the rendered rename order. The old _ts entry read ts, not _ts.
			if lookup("ts", fields, default: true) == lookup("ts", fields, default: false) {
			  ."time" = ."ts"
			  del ."ts"
			  present["time"] = true
			}
			if lookup("exception.message", fields, default: true) == lookup("exception.message", fields, default: false) {
			  ."exception_message" = ."exception.message"
			  del ."exception.message"
			  present["exception_message"] = true
			}
			if lookup("exception.stacktrace", fields, default: true) == lookup("exception.stacktrace", fields, default: false) {
			  ."exception_stacktrace" = ."exception.stacktrace"
			  del ."exception.stacktrace"
			  present["exception_stacktrace"] = true
			}
			if lookup("exception.type", fields, default: true) == lookup("exception.type", fields, default: false) {
			  ."exception_type" = ."exception.type"
			  del ."exception.type"
			  present["exception_type"] = true
			}
			if lookup("log", fields, default: true) == lookup("log", fields, default: false) {
			  ."message" = ."log"
			  del ."log"
			  present["message"] = true
			}
			if lookup("msg", fields, default: true) == lookup("msg", fields, default: false) {
			  ."message" = ."msg"
			  del ."msg"
			  present["message"] = true
			}

			# These selectors address nested fields, unlike the literal exception.* keys.
			parent = ."syslog"
			parent_text = to_string(parent)
			# Only a non-string object renders with "{".
			if parent != parent_text && slice(parent_text, 0, end: 1) == "{" {
			  if lookup("identifier", parent, default: true) == lookup("identifier", parent, default: false) {
			    .service = ."syslog"."identifier"
			    del ."syslog"."identifier"
			    present["service"] = true
			  }
			}
			parent = ."systemd"
			parent_text = to_string(parent)
			# Only a non-string object renders with "{".
			if parent != parent_text && slice(parent_text, 0, end: 1) == "{" {
			  if lookup("unit", parent, default: true) == lookup("unit", parent, default: false) {
			    .service = ."systemd"."unit"
			    del ."systemd"."unit"
			    present["service"] = true
			  }
			}

			# Preserve existing destinations, including explicit null, and retain the
			# source on collisions. eventrouter's service label has first priority.
			if !present["service"] && lookup("bozon_eventrouter_service", fields, default: true) == lookup("bozon_eventrouter_service", fields, default: false) {
			  ."service" = ."bozon_eventrouter_service"
			  del ."bozon_eventrouter_service"
			  present["service"] = true
			}
			if !present["service"] && lookup("k8s_label_app", fields, default: true) == lookup("k8s_label_app", fields, default: false) {
			  ."service" = ."k8s_label_app"
			  del ."k8s_label_app"
			  present["service"] = true
			}
			if !present["ci_job_id"] && lookup("k8s_label_jobid", fields, default: true) == lookup("k8s_label_jobid", fields, default: false) {
			  ."ci_job_id" = ."k8s_label_jobid"
			  del ."k8s_label_jobid"
			  present["ci_job_id"] = true
			}
			if !present["host"] && lookup("k8s_node", fields, default: true) == lookup("k8s_node", fields, default: false) {
			  ."host" = ."k8s_node"
			  del ."k8s_node"
			  present["host"] = true
			}
			if !present["zone"] && lookup("k8s_node_label_topology.kubernetes.io/zone", fields, default: true) == lookup("k8s_node_label_topology.kubernetes.io/zone", fields, default: false) {
			  ."zone" = ."k8s_node_label_topology.kubernetes.io/zone"
			  del ."k8s_node_label_topology.kubernetes.io/zone"
			  present["zone"] = true
			}
			if !present["service"] && lookup("k8s_pod_label_app", fields, default: true) == lookup("k8s_pod_label_app", fields, default: false) {
			  ."service" = ."k8s_pod_label_app"
			  del ."k8s_pod_label_app"
			  present["service"] = true
			}
			if !present["ci_job_id"] && lookup("k8s_pod_label_jobid", fields, default: true) == lookup("k8s_pod_label_jobid", fields, default: false) {
			  ."ci_job_id" = ."k8s_pod_label_jobid"
			  del ."k8s_pod_label_jobid"
			  present["ci_job_id"] = true
			}

			# match_fields used AsString(): missing, empty string, object and array
			# match ""; explicit null, booleans and numbers do not.
			lt = ."@lt"
			lt_text = to_string(lt)
			if !present["@lt"] || lt == "" {
			  ."@lt" = "ok"
			} else if lt != lt_text && parse_regex(lt_text, r'^[\[{]') != null {
			  ."@lt" = "ok"
			}

			# Move event_message -> message ahead of flatten to avoid another transform.
			# For eventrouter, event.message would overwrite event_message during flatten.
			# Consume that nested source now only when the message destination is absent;
			# otherwise leave both sources intact for the existing flatten actions.
			if !present["message"] {
			  if .service == "eventrouter" {
			    event = .event
			    event_text = to_string(event)
			    if event != event_text && slice(event_text, 0, end: 1) == "{" {
			      if lookup("message", event, default: true) == lookup("message", event, default: false) {
			        .message = .event.message
			        del .event.message
			        del .event_message
			        present["message"] = true
			      }
			    }
			  }
			  if !present["message"] && present["event_message"] {
			    .message = .event_message
			    del .event_message
			  }
			}
		`,
		inputs: transformProductionNormalizationInputs(),
	}
	large := base
	large.name = "large_event"
	large.inputs = make([]transformCallCase, 0, len(base.inputs))
	for _, sample := range base.inputs {
		input := decodeTransformCallFields(tb, sample.input)
		want := decodeTransformCallFields(tb, sample.want)
		// Unrelated nested fields expose the cost of the full root snapshot.
		// Both corpora retain the same normalization cases and expected changes.
		for i := range 64 {
			key := fmt.Sprintf("context_%02d", i)
			field := map[string]any{
				"name":    key,
				"active":  true,
				"samples": []int{1, 2, 3, 4},
				"labels": map[string]any{
					"team": "observability",
					"zone": "z502",
				},
			}
			input[key] = field
			want[key] = field
		}
		large.inputs = append(large.inputs, transformEventCase(sample.name, input, want))
	}
	return []transformCallScenario{base, large}
}

// Expected complete events are specified independently of transform execution.
// Every input has weight 1/8; these weights do not represent production traffic.
func transformProductionNormalizationInputs() []transformCallCase {
	inputs := []transformCallCase{
		{
			name: "ordered_sources",
			input: `{
				"log": "decoded message",
				"msg": "preferred message",
				"ts": "2026-11-10T18:20:00Z",
				"exception.message": "bad request",
				"exception.stacktrace": "at main",
				"exception.type": "Error",
				"k8s_label_app": "gateway",
				"k8s_pod_label_app": "fallback",
				"k8s_label_jobid": "job-1",
				"k8s_node": "node-1",
				"k8s_node_label_topology.kubernetes.io/zone": "z502",
				"keep": 7
			}`,
			want: `{
				"message": "preferred message",
				"time": "2026-11-10T18:20:00Z",
				"exception_message": "bad request",
				"exception_stacktrace": "at main",
				"exception_type": "Error",
				"service": "gateway",
				"k8s_pod_label_app": "fallback",
				"ci_job_id": "job-1",
				"host": "node-1",
				"zone": "z502",
				"@lt": "ok",
				"keep": 7
			}`,
		},
		{
			name: "null_destinations",
			input: `{
				"message": null,
				"service": null,
				"host": null,
				"ci_job_id": null,
				"zone": null,
				"@lt": null,
				"event_message": "untouched",
				"bozon_eventrouter_service": "eventrouter",
				"k8s_label_app": "gateway",
				"k8s_pod_label_app": "pod",
				"k8s_node": "node",
				"k8s_label_jobid": "job",
				"k8s_pod_label_jobid": "job-2",
				"k8s_node_label_topology.kubernetes.io/zone": "z502",
				"keep": false
			}`,
			want: `{
				"message": null,
				"service": null,
				"host": null,
				"ci_job_id": null,
				"zone": null,
				"@lt": null,
				"event_message": "untouched",
				"bozon_eventrouter_service": "eventrouter",
				"k8s_label_app": "gateway",
				"k8s_pod_label_app": "pod",
				"k8s_node": "node",
				"k8s_label_jobid": "job",
				"k8s_pod_label_jobid": "job-2",
				"k8s_node_label_topology.kubernetes.io/zone": "z502",
				"keep": false
			}`,
		},
		{
			name: "nested_sources",
			input: `{
				"syslog": {"identifier": "sys", "pid": 17},
				"systemd": {"unit": "unit", "state": "running"},
				"service": "old",
				"@lt": {},
				"keep": "yes"
			}`,
			want: `{
				"syslog": {"pid": 17},
				"systemd": {"state": "running"},
				"service": "unit",
				"@lt": "ok",
				"keep": "yes"
			}`,
		},
		{
			name: "eventrouter_nested_message",
			input: `{
				"bozon_eventrouter_service": "eventrouter",
				"k8s_label_app": "fallback",
				"event": {"message": {"text": "nested", "code": 7}, "kind": "Added"},
				"event_message": "flat",
				"@lt": []
			}`,
			want: `{
				"service": "eventrouter",
				"k8s_label_app": "fallback",
				"message": {"text": "nested", "code": 7},
				"event": {"kind": "Added"},
				"@lt": "ok"
			}`,
		},
		{
			name: "eventrouter_existing_message",
			input: `{
				"service": "eventrouter",
				"message": "keep",
				"event": {"message": "nested", "kind": "Added"},
				"event_message": "flat",
				"@lt": ""
			}`,
			want: `{
				"service": "eventrouter",
				"message": "keep",
				"event": {"message": "nested", "kind": "Added"},
				"event_message": "flat",
				"@lt": "ok"
			}`,
		},
		{
			name: "scalar_parent_and_null_message",
			input: `{
				"event_message": null,
				"syslog": 42,
				"systemd": ["unit"],
				"@lt": false,
				"keep": {"x": 1}
			}`,
			want: `{
				"message": null,
				"syslog": 42,
				"systemd": ["unit"],
				"@lt": false,
				"keep": {"x": 1}
			}`,
		},
		{
			name:  "missing_sources",
			input: `{"keep":{"count":7}}`,
			want:  `{"keep":{"count":7},"@lt":"ok"}`,
		},
		{
			name: "null_sources_and_unicode",
			input: `{
				"bozon_eventrouter_service": null,
				"k8s_label_app": "fallback",
				"k8s_pod_label_app": "pod",
				"k8s_label_jobid": null,
				"k8s_pod_label_jobid": "job-2",
				"k8s_node": null,
				"k8s_node_label_topology.kubernetes.io/zone": null,
				"@lt": 17,
				"event_message": "строка"
			}`,
			want: `{
				"service": null,
				"k8s_label_app": "fallback",
				"k8s_pod_label_app": "pod",
				"ci_job_id": null,
				"k8s_pod_label_jobid": "job-2",
				"host": null,
				"zone": null,
				"@lt": 17,
				"message": "строка"
			}`,
		},
	}
	return inputs
}
