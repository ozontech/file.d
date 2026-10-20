package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransformBenchmarkReport(t *testing.T) {
	tests := []struct {
		name       string
		transform  string
		modify     string
		comparison string
	}{
		{
			name:       "improvement",
			transform:  "1000.0 ns/op 1000 B/op 10 allocs/op",
			modify:     "200.0 ns/op 200 B/op 2 allocs/op",
			comparison: "-80.0% ns/op, -80.0% B/op, -80.0% allocs/op",
		},
		{
			name:       "regression",
			transform:  "100.0 ns/op 200 B/op 2 allocs/op",
			modify:     "150.0 ns/op 300 B/op 4 allocs/op",
			comparison: "+50.0% ns/op, +50.0% B/op, +100.0% allocs/op",
		},
		{
			name:       "no_allocations",
			transform:  "100.0 ns/op 0 B/op 0 allocs/op",
			modify:     "100.0 ns/op 0 B/op 0 allocs/op",
			comparison: "0.0% ns/op, 0.0% B/op, 0.0% allocs/op",
		},
		{
			name:       "zero_base",
			transform:  "100.0 ns/op 0 B/op 0 allocs/op",
			modify:     "50.0 ns/op 32 B/op 1 allocs/op",
			comparison: "-50.0% ns/op, n/a B/op, n/a allocs/op",
		},
		{
			name:       "rounded_zero",
			transform:  "100000.0 ns/op 200 B/op 2 allocs/op",
			modify:     "99999.0 ns/op 0 B/op 0 allocs/op",
			comparison: "0.0% ns/op, -100.0% B/op, -100.0% allocs/op",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			transform := "BenchmarkTransformCalls/trim_one/transform-8 1000 " + tc.transform
			modify := "BenchmarkTransformCalls/trim_one/modify-8 5000 " + tc.modify
			output, err := formatResults("goos: linux\n" + transform + "\n" + modify + "\nPASS\n")
			require.NoError(t, err)
			lines := strings.Split(strings.TrimSpace(output), "\n")
			require.Equal(t, "goos: linux", lines[0])
			require.Equal(t, strings.Fields(transform), strings.Fields(lines[1]))
			require.Equal(t, strings.Fields(modify), strings.Fields(lines[2]))
			require.Equal(t, "modify vs transform: "+tc.comparison, lines[3])
			require.Equal(t, "PASS", lines[len(lines)-1])
			for _, unit := range []string{"ns/op", "B/op", "allocs/op"} {
				require.Equal(t, strings.Index(lines[1], unit), strings.Index(lines[2], unit))
			}
		})
	}
}

func TestTransformBenchmarkReportPairsRepetitionsAndCPUs(t *testing.T) {
	tests := []struct {
		name            string
		comparator      string
		comparatorFirst bool
	}{
		{name: "modify", comparator: "modify"},
		{name: "rename", comparator: "rename"},
		{name: "chain", comparator: "modify_x64"},
		{name: "comparator_first", comparator: "modify_x4", comparatorFirst: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var input strings.Builder
			variants := []string{"transform", tc.comparator}
			if tc.comparatorFirst {
				variants[0], variants[1] = variants[1], variants[0]
			}
			// Go emits all repetitions/CPU settings of a variant together.
			for _, variant := range variants {
				for _, cpu := range []string{"", "-8"} {
					for repeat := 1; repeat <= 2; repeat++ {
						value := repeat * 100
						if cpu != "" {
							value *= 3
						}
						if variant != "transform" {
							value /= 2
						}
						fmt.Fprintf(&input, "BenchmarkTransformCalls/mapping/mixed/%s%s 1000 %d ns/op 0 B/op 0 allocs/op\n",
							variant, cpu, value)
					}
				}
			}
			output, err := formatResults(input.String())
			require.NoError(t, err)
			groups := strings.Split(strings.TrimSpace(output), "\n\n")
			require.Len(t, groups, 4)
			for i, group := range groups {
				lines := strings.Split(group, "\n")
				require.Len(t, lines, 3)
				cpu := ""
				if i >= 2 {
					cpu = "-8"
				}
				require.Equal(t, "BenchmarkTransformCalls/mapping/mixed/transform"+cpu, strings.Fields(lines[0])[0])
				require.Equal(t, "BenchmarkTransformCalls/mapping/mixed/"+tc.comparator+cpu, strings.Fields(lines[1])[0])
				require.Equal(t, tc.comparator+" vs transform: -50.0% ns/op, 0.0% B/op, 0.0% allocs/op", lines[2])
			}
		})
	}
}

func TestTransformBenchmarkReportRejectsInvalidResults(t *testing.T) {
	const transform = "BenchmarkTransformCalls/trim_one/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op\n"
	const modify = "BenchmarkTransformCalls/trim_one/modify-8 1000 50.0 ns/op 0 B/op 0 allocs/op\n"
	const standalone = "BenchmarkTransformOnly/loop/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op\n"
	tests := []struct {
		name  string
		input string
	}{
		{name: "no_results", input: "PASS\n"},
		{name: "missing_modify", input: transform},
		{name: "missing_transform", input: modify},
		{name: "standalone_does_not_hide_missing_pair", input: standalone + transform},
		{name: "different_scenario", input: transform + strings.ReplaceAll(modify, "trim_one", "full")},
		{name: "different_cpu", input: transform + strings.ReplaceAll(modify, "-8", "-4")},
		{name: "incomplete_repetition", input: transform + transform + modify},
		{name: "extra_comparator", input: transform + modify + modify},
		{name: "multiple_comparators", input: transform + transform + modify + strings.ReplaceAll(modify, "modify", "rename")},
		{name: "invalid_metric", input: transform + strings.ReplaceAll(modify, "50.0", "invalid")},
		{name: "negative_metric", input: transform + strings.ReplaceAll(modify, "50.0", "-50.0")},
		{name: "nan_metric", input: transform + strings.ReplaceAll(modify, "50.0", "NaN")},
		{name: "infinite_metric", input: transform + strings.ReplaceAll(modify, "50.0", "+Inf")},
		{name: "wrong_units", input: transform + strings.ReplaceAll(modify, "ns/op", "ms/op")},
		{name: "unknown_variant", input: transform + strings.ReplaceAll(modify, "modify", "unknown")},
		{name: "invalid_cpu", input: transform + strings.ReplaceAll(modify, "-8", "-invalid")},
		{name: "invalid_iterations", input: transform + strings.ReplaceAll(modify, "1000", "invalid")},
		{name: "zero_iterations", input: transform + strings.ReplaceAll(modify, "1000", "0")},
		{name: "zero_chain_length", input: transform + strings.ReplaceAll(modify, "modify", "modify_x0")},
		{name: "one_action_is_not_chain", input: transform + strings.ReplaceAll(modify, "modify", "modify_x1")},
		{name: "invalid_chain_length", input: transform + strings.ReplaceAll(modify, "modify", "modify_xmany")},
		{name: "leading_zero_chain_length", input: transform + strings.ReplaceAll(modify, "modify", "modify_x02")},
		{name: "empty_scenario", input: strings.ReplaceAll(transform, "trim_one", "")},
		{name: "empty_nested_scenario", input: strings.ReplaceAll(transform, "trim_one", "mapping//mixed")},
		{name: "standalone_comparator", input: strings.ReplaceAll(standalone, "/transform-", "/rename-")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output, err := formatResults(tc.input)
			require.Error(t, err)
			require.Equal(t, tc.input, output, "preserve diagnostics on failure")
		})
	}
}

func TestTransformBenchmarkReportStandalone(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "one_result",
			input: "BenchmarkTransformOnly/loop/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op\n",
		},
		{
			name: "repetitions_and_cpus",
			input: "BenchmarkTransformOnly/loop/transform 1000 100.0 ns/op 32 B/op 1 allocs/op\n" +
				"BenchmarkTransformOnly/loop/transform 2000 110.0 ns/op 32 B/op 1 allocs/op\n" +
				"BenchmarkTransformOnly/loop/transform-8 1000 120.0 ns/op 32 B/op 1 allocs/op\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			output, err := formatResults(tc.input)
			require.NoError(t, err)
			require.Equal(t, strings.Fields(tc.input), strings.Fields(output))
			require.NotContains(t, output, "vs transform")
			require.NotContains(t, output, "%")
		})
	}
}

func TestTransformBenchmarkReportOrderAndAlignment(t *testing.T) {
	longScenario := "tags/" + strings.Repeat("long_name_", 8) + "/mixed"
	rows := []string{
		"BenchmarkTransformCalls/assign/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op",
		"BenchmarkTransformCalls/assign/rename-8 2000 50.0 ns/op 0 B/op 0 allocs/op",
		"BenchmarkTransformCalls/" + longScenario + "/transform-8 1000000000000 100000000000.0 ns/op 100000000000 B/op 100000000000 allocs/op",
		"BenchmarkTransformCalls/" + longScenario + "/modify_x64-8 2000 50.0 ns/op 0 B/op 0 allocs/op",
		// Same scenario name in the standalone group must not collide with a pair.
		"BenchmarkTransformOnly/assign/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op",
	}
	input := "goos: linux\n" + strings.Join(rows, "\n") + "\nPASS\n"
	output, err := formatResults(input)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(output, "goos: linux\n"))
	require.True(t, strings.HasSuffix(output, "PASS\n"))
	var formatted []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "Benchmark") {
			formatted = append(formatted, line)
		}
	}
	require.Len(t, formatted, len(rows))
	for i, row := range rows {
		require.Equal(t, strings.Fields(row), strings.Fields(formatted[i]))
		for _, unit := range []string{"ns/op", "B/op", "allocs/op"} {
			require.Equal(t, strings.Index(formatted[0], unit), strings.Index(formatted[i], unit))
		}
	}
	require.Contains(t, output, "rename vs transform:")
	require.Contains(t, output, "modify_x64 vs transform:")
	require.Equal(t, 2, strings.Count(output, "vs transform:"))
}
