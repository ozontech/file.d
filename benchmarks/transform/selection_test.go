package transformbench

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type transformBenchmarkSelection struct {
	calls []transformCallScenario
	only  []transformCallScenario
}

var selectedTransformBenchmarks transformBenchmarkSelection

// Representative workloads for assessing optimizations. Keep this set explicit
// so adding a workload does not extend the default run. Execution keeps fixture order.
var transformSmokeCases = []string{
	"assign_new5",
	"trim4",
	"rename/preserve/5",
	"logs/no_regex/mixed",
	"logs/regex/mixed",
	"chains/mapping/16/mixed",
	"chains/tags/16/mixed",
	"dynamic/trim_slice/mixed",
	"collections/filter_sum/64",
}

func TestMain(m *testing.M) {
	// Plain go test retains the full suite. The report command explicitly
	// selects the representative smoke set by default.
	group := flag.String("transform.group", "all", "benchmark group: smoke, single, chains, only, all")
	prefix := flag.String("transform.case", "", "scenario path or prefix within the group")
	list := flag.Bool("transform.list", false, "list selected scenario paths without running plugins")
	flag.Parse()

	var err error
	selectedTransformBenchmarks, err = selectTransformBenchmarks(*group, *prefix,
		transformCallScenarios(), transformOnlyScenarios())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if *list {
		for _, scenario := range selectedTransformBenchmarks.calls {
			fmt.Fprintln(os.Stdout, "CASE", transformBenchmarkCase(*group, scenario.name))
		}
		for _, scenario := range selectedTransformBenchmarks.only {
			fmt.Fprintln(os.Stdout, "CASE", transformBenchmarkCase(*group, scenario.name))
		}
		return
	}
	os.Exit(m.Run())
}

func selectTransformBenchmarks(group, prefix string, calls, only []transformCallScenario) (transformBenchmarkSelection, error) {
	var selected transformBenchmarkSelection
	var exact transformBenchmarkSelection
	switch group {
	case "smoke", "single", "chains", "only", "all":
	default:
		return selected, fmt.Errorf("unknown GROUP %q; choose smoke, single, chains, only, or all", group)
	}
	if prefix != "" {
		for _, segment := range strings.Split(prefix, "/") {
			if segment == "" || segment == "." || segment == ".." {
				return selected, fmt.Errorf("invalid CASE %q: use a scenario path without empty, . or .. segments", prefix)
			}
		}
	}
	for _, scenario := range calls {
		if group == "only" || (group == "single" && len(scenario.actions) != 1) ||
			(group == "chains" && len(scenario.actions) <= 1) {
			continue
		}
		if matchesTransformBenchmark(group, prefix, scenario.name) {
			selected.calls = append(selected.calls, scenario)
			if transformBenchmarkCase(group, scenario.name) == prefix {
				exact.calls = append(exact.calls, scenario)
			}
		}
	}
	if group == "smoke" || group == "only" || group == "all" {
		for _, scenario := range only {
			if matchesTransformBenchmark(group, prefix, scenario.name) {
				selected.only = append(selected.only, scenario)
				if transformBenchmarkCase(group, scenario.name) == prefix {
					exact.only = append(exact.only, scenario)
				}
			}
		}
	}
	// Some scenario names are also prefixes of others, such as
	// rename/preserve/5 and rename/preserve/5/large_event. A listed full
	// path must select that exact scenario; otherwise CASE selects a family.
	if len(exact.calls)+len(exact.only) != 0 {
		return exact, nil
	}
	if len(selected.calls)+len(selected.only) == 0 {
		return selected, fmt.Errorf("no scenarios match GROUP=%s CASE=%q; list available paths with make bench-transform GROUP=%s LIST=1", group, prefix, group)
	}
	return selected, nil
}

func matchesTransformBenchmark(group, prefix, name string) bool {
	if group == "smoke" && !slices.Contains(transformSmokeCases, name) {
		return false
	}
	path := transformBenchmarkCase(group, name)
	return prefix == "" || path == prefix || strings.HasPrefix(path, prefix+"/")
}

func transformBenchmarkCase(group, name string) string {
	if group == "chains" {
		return strings.TrimPrefix(name, "chains/")
	}
	return name
}

func TestTransformBenchmarkSelection(t *testing.T) {
	calls := []transformCallScenario{
		{
			name:    "assign_new5",
			actions: make([]transformCallAction, 1),
		},
		{
			name:    "chains/mapping/16/first",
			actions: make([]transformCallAction, 16),
		},
		{
			name:    "chains/mapping/16/mixed",
			actions: make([]transformCallAction, 16),
		},
		{
			name:    "chains/mapping/160/mixed",
			actions: make([]transformCallAction, 160),
		},
		{
			name:    "strings/trim/right/128B",
			actions: make([]transformCallAction, 1),
		},
		{
			name:    "rename/preserve/5",
			actions: make([]transformCallAction, 1),
		},
	}
	only := []transformCallScenario{
		{
			name: "collections/filter_sum/64",
		},
		{
			name: "collections/filter_sum/640",
		},
	}
	type testCase struct {
		name   string
		group  string
		prefix string
		calls  []int
		only   []int
	}
	tests := []testCase{
		{
			name:  "all_preserves_order",
			group: "all",
			calls: []int{0, 1, 2, 3, 4, 5},
			only:  []int{0, 1},
		},
		{
			name:  "single_includes_rename",
			group: "single",
			calls: []int{0, 4, 5},
		},
		{
			name:  "chains",
			group: "chains",
			calls: []int{1, 2, 3},
		},
		{
			name:  "only",
			group: "only",
			only:  []int{0, 1},
		},
		{
			name:  "smoke_uses_fixture_order",
			group: "smoke",
			calls: []int{0, 2, 5},
			only:  []int{0},
		},
		{
			name:   "family",
			group:  "chains",
			prefix: "mapping",
			calls:  []int{1, 2, 3},
		},
		{
			name:   "prefix_matches_whole_segments",
			group:  "chains",
			prefix: "mapping/16",
			calls:  []int{1, 2},
		},
		{
			name:   "exact_chain",
			group:  "chains",
			prefix: "mapping/16/mixed",
			calls:  []int{2},
		},
		{
			name:   "exact_single",
			group:  "single",
			prefix: "strings/trim/right/128B",
			calls:  []int{4},
		},
		{
			name:   "exact_standalone",
			group:  "only",
			prefix: "collections/filter_sum/64",
			only:   []int{0},
		},
		{
			name:   "all_uses_full_path",
			group:  "all",
			prefix: "chains/mapping/16",
			calls:  []int{1, 2},
		},
		{
			name:   "smoke_is_filtered_without_expanding",
			group:  "smoke",
			prefix: "chains/mapping",
			calls:  []int{2},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			selected, err := selectTransformBenchmarks(tc.group, tc.prefix, calls, only)
			require.NoError(t, err)
			require.Len(t, selected.calls, len(tc.calls))
			require.Len(t, selected.only, len(tc.only))
			for i, index := range tc.calls {
				// Selection keeps the complete scenario, including every action.
				require.Equal(t, calls[index], selected.calls[i])
			}
			for i, index := range tc.only {
				require.Equal(t, only[index], selected.only[i])
			}
		})
	}
}

func TestTransformBenchmarkSelectionRejectsInvalidPaths(t *testing.T) {
	type testCase struct {
		name   string
		group  string
		prefix string
		err    string
	}
	tests := []testCase{
		{
			name:  "unknown_group",
			group: "unknown",
			err:   "unknown GROUP",
		},
		{
			name:   "unknown_case",
			group:  "chains",
			prefix: "missing",
			err:    "GROUP=chains LIST=1",
		},
		{
			name:   "group_mismatch",
			group:  "single",
			prefix: "collections",
			err:    "no scenarios match",
		},
		{
			name:   "no_regex_matching",
			group:  "chains",
			prefix: "mapping/.*",
			err:    "no scenarios match",
		},
		{
			name:   "cannot_select_one_variant",
			group:  "single",
			prefix: "trim4/modify",
			err:    "no scenarios match",
		},
		{
			name:   "empty_segment",
			group:  "all",
			prefix: "chains//mapping",
			err:    "invalid CASE",
		},
		{
			name:   "leading_slash",
			group:  "all",
			prefix: "/chains",
			err:    "invalid CASE",
		},
		{
			name:   "trailing_slash",
			group:  "all",
			prefix: "chains/",
			err:    "invalid CASE",
		},
		{
			name:   "parent_segment",
			group:  "all",
			prefix: "chains/../mapping",
			err:    "invalid CASE",
		},
	}
	calls, only := transformCallScenarios(), transformOnlyScenarios()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := selectTransformBenchmarks(tc.group, tc.prefix, calls, only)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestTransformSmokeCasesExist(t *testing.T) {
	selected, err := selectTransformBenchmarks("smoke", "", transformCallScenarios(), transformOnlyScenarios())
	require.NoError(t, err)
	var names []string
	for _, scenario := range selected.calls {
		names = append(names, scenario.name)
	}
	for _, scenario := range selected.only {
		names = append(names, scenario.name)
	}
	require.ElementsMatch(t, transformSmokeCases, names, "renaming a control must update the smoke set")
}

func TestTransformBenchmarkListedPathsSelectTheirScenario(t *testing.T) {
	calls, only := transformCallScenarios(), transformOnlyScenarios()
	groups := []string{"smoke", "single", "chains", "only", "all"}
	for _, group := range groups {
		t.Run(group, func(t *testing.T) {
			selected, err := selectTransformBenchmarks(group, "", calls, only)
			require.NoError(t, err)
			scenarios := slices.Clone(selected.calls)
			scenarios = append(scenarios, selected.only...)
			for _, scenario := range scenarios {
				path := transformBenchmarkCase(group, scenario.name)
				filtered, err := selectTransformBenchmarks(group, path, calls, only)
				require.NoError(t, err)
				require.Equal(t, 1, len(filtered.calls)+len(filtered.only), path)
			}
		})
	}
}
