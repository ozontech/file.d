package transformbench

import (
	"fmt"
	"maps"
	"strings"

	"github.com/ozontech/file.d/plugin/action/modify"
)

func transformMappingScenarios() []transformCallScenario {
	// Synthetic operation names have no derivation from the numeric suffix
	// of a code: this is a table lookup, not a prefix/suffix substitution.
	names := []string{
		"create_order", "cancel_order", "get_order", "list_orders",
		"submit_payment", "refund_payment", "create_invoice", "send_invoice",
		"reserve_stock", "release_stock", "add_product", "update_product",
		"remove_product", "list_products", "get_inventory", "sync_inventory",
		"create_user", "update_user", "delete_user", "get_profile",
		"list_users", "add_group", "remove_group", "list_groups",
		"start_session", "refresh_token", "revoke_token", "verify_email",
		"reset_password", "enable_mfa", "disable_mfa", "check_permission",
		"upload_file", "download_file", "delete_file", "list_files",
		"copy_file", "move_file", "get_metadata", "set_metadata",
		"enqueue_job", "cancel_job", "retry_job", "get_job",
		"list_jobs", "pause_queue", "resume_queue", "purge_queue",
		"write_metric", "read_metric", "query_metrics", "create_alert",
		"update_alert", "delete_alert", "list_alerts", "acknowledge_alert",
		"send_message", "receive_message", "archive_message", "create_channel",
		"join_channel", "leave_channel", "list_channels", "mark_read",
	}
	var scenarios []transformCallScenario
	for _, count := range []int{4, 16, 64} {
		// Codes and replacement names are disjoint. Each conditional modify
		// reads the same field it writes, so a replacement must not trigger
		// another rule. This workload does not model arbitrary cyclic maps.
		var entries []string
		var actions []transformCallAction
		var matches []transformCallCase
		for i, name := range names[:count] {
			code := fmt.Sprintf("code_%02d", i)
			entries = append(entries, fmt.Sprintf("    %q: %q", code, name))
			actions = append(actions, transformCallAction{
				config: &modify.Config{"operation": name},
				doIf: map[string]any{
					"op": "equal", "field": "operation", "values": code, "case_sensitive": true,
				},
			})
			matches = append(matches, transformMappingCase(fmt.Sprintf("entry_%02d", i), code, name))
		}
		miss := transformMappingCase("no_match", "code_xx", "code_xx")
		base := transformCallScenario{
			source: fmt.Sprintf(`
if .operation != null {
  .operation = lookup(.operation, {
%s
  })
}
`, strings.Join(entries, ",\n")),
			actions: actions,
			// Check every entry, not only the measured first and last ones.
			cases: append(matches,
				transformMappingCase("empty", "", ""),
				transformMappingCase("case_sensitive", "CODE_00", "CODE_00"),
				transformMappingCase("already_mapped", names[0], names[0]),
				transformCallCase{
					name:  "missing",
					input: `{"keep":true}`,
					want:  `{"keep":true}`,
				},
				transformCallCase{
					name:  "null",
					input: `{"operation":null,"keep":42}`,
					want:  `{"operation":null,"keep":42}`,
				}),
		}
		profiles := []transformCallProfile{
			{
				name:   "first",
				inputs: matches[:1],
			},
			{
				name:   "last",
				inputs: matches[count-1:],
			},
			{
				name:   "no_match",
				inputs: []transformCallCase{miss},
			},
			// 1/3 first match, 1/3 last match, 1/3 unknown, in this order.
			{
				name:   "mixed",
				inputs: []transformCallCase{matches[0], matches[count-1], miss},
			},
		}
		for _, profile := range profiles {
			scenario := base
			scenario.name = fmt.Sprintf("chains/mapping/%d/%s", count, profile.name)
			scenario.inputs = profile.inputs
			scenarios = append(scenarios, scenario)
		}
	}
	return scenarios
}

func transformMappingCase(name, code, mapped string) transformCallCase {
	input := transformFieldEvent(false)
	input["operation"] = code
	want := maps.Clone(input)
	want["operation"] = mapped
	return transformEventCase(name, input, want)
}
