package transformbench

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// BenchmarkTransformFieldNormalization uses the same inputs and expected events
// as the original production normalization program, with exists and rename.
func BenchmarkTransformFieldNormalization(b *testing.B) {
	scenarios := transformFieldNormalizationScenarios(b)
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			variants := scenario.variants(b)
			require.Len(b, variants, 1)
			benchmarkTransformCallVariant(b, scenario.inputs, variants[0])
		})
	}
}

func TestTransformFieldNormalizationScenarios(t *testing.T) {
	scenarios := transformFieldNormalizationScenarios(t)
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			checkTransformCallScenario(t, &scenario)
		})
	}
}

func transformFieldNormalizationScenarios(tb testing.TB) []transformCallScenario {
	tb.Helper()
	scenarios := transformProductionNormalizationScenarios(tb)
	for i := range scenarios {
		scenarios[i].source = `
			# Preserve the explicit priority of the original normalization program.
			rename(.ts, .time, override: true)
			rename(."exception.message", .exception_message, override: true)
			rename(."exception.stacktrace", .exception_stacktrace, override: true)
			rename(."exception.type", .exception_type, override: true)
			rename(.log, .message, override: true)
			rename(.msg, .message, override: true)
			rename(.syslog.identifier, .service, override: true)
			rename(.systemd.unit, .service, override: true)

			# Existing destinations, including null, preserve their source fields.
			rename(.bozon_eventrouter_service, .service)
			rename(.k8s_label_app, .service)
			rename(.k8s_label_jobid, .ci_job_id)
			rename(.k8s_node, .host)
			rename(."k8s_node_label_topology.kubernetes.io/zone", .zone)
			rename(.k8s_pod_label_app, .service)
			rename(.k8s_pod_label_jobid, .ci_job_id)

			# Keep the original match_fields behavior for null, scalars and containers.
			lt = ."@lt"
			lt_text = to_string(lt)
			if !exists(."@lt") || lt == "" {
				."@lt" = "ok"
			} else if lt != lt_text && parse_regex(lt_text, r'^[\[{]') != null {
				."@lt" = "ok"
			}

			# Consume the nested eventrouter message only when it was actually moved.
			if .service == "eventrouter" && rename(.event.message, .message) {
				del .event_message
			}
			rename(.event_message, .message)
		`
	}
	return scenarios
}
