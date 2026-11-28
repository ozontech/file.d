package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransformBenchmarkReport(t *testing.T) {
	for _, tc := range []struct {
		name, transform, modify, comparison string
	}{
		{"improvement", "1000.0 ns/op 1000 B/op 10 allocs/op", "200.0 ns/op 200 B/op 2 allocs/op",
			"-80.0% ns/op, -80.0% B/op, -80.0% allocs/op"},
		{"regression", "100.0 ns/op 200 B/op 2 allocs/op", "150.0 ns/op 300 B/op 4 allocs/op",
			"+50.0% ns/op, +50.0% B/op, +100.0% allocs/op"},
		{"no_allocations", "100.0 ns/op 0 B/op 0 allocs/op", "100.0 ns/op 0 B/op 0 allocs/op",
			"0.0% ns/op, 0.0% B/op, 0.0% allocs/op"},
		{"zero_base", "100.0 ns/op 0 B/op 0 allocs/op", "50.0 ns/op 32 B/op 1 allocs/op",
			"-50.0% ns/op, n/a B/op, n/a allocs/op"},
		{"rounded_zero", "100000.0 ns/op 200 B/op 2 allocs/op", "99999.0 ns/op 0 B/op 0 allocs/op",
			"0.0% ns/op, -100.0% B/op, -100.0% allocs/op"},
	} {
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
	var input strings.Builder
	// Go prints all repetitions/CPU settings of transform before those of modify.
	for _, variant := range []string{"transform", "modify"} {
		for _, cpu := range []string{"", "-8"} {
			for repeat := 1; repeat <= 2; repeat++ {
				value := repeat * 100
				if cpu != "" {
					value *= 3
				}
				if variant == "modify" {
					value /= 2
				}
				fmt.Fprintf(&input, "BenchmarkTransformCalls/trim_one/%s%s 1000 %d ns/op 0 B/op 0 allocs/op\n",
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
		require.Equal(t, "BenchmarkTransformCalls/trim_one/transform"+cpu, strings.Fields(lines[0])[0])
		require.Equal(t, "BenchmarkTransformCalls/trim_one/modify"+cpu, strings.Fields(lines[1])[0])
		require.Equal(t, "modify vs transform: -50.0% ns/op, 0.0% B/op, 0.0% allocs/op", lines[2])
	}
}

func TestTransformBenchmarkReportRejectsInvalidResults(t *testing.T) {
	const transform = "BenchmarkTransformCalls/trim_one/transform-8 1000 100.0 ns/op 32 B/op 1 allocs/op\n"
	const modify = "BenchmarkTransformCalls/trim_one/modify-8 1000 50.0 ns/op 0 B/op 0 allocs/op\n"
	for _, tc := range []struct {
		name, input string
	}{
		{"no_results", "PASS\n"},
		{"missing_modify", transform},
		{"missing_transform", modify},
		{"different_scenario", transform + strings.ReplaceAll(modify, "trim_one", "full")},
		{"different_cpu", transform + strings.ReplaceAll(modify, "-8", "-4")},
		{"incomplete_repetition", transform + transform + modify},
		{"invalid_metric", transform + strings.ReplaceAll(modify, "50.0", "invalid")},
		{"negative_metric", transform + strings.ReplaceAll(modify, "50.0", "-50.0")},
		{"nan_metric", transform + strings.ReplaceAll(modify, "50.0", "NaN")},
		{"infinite_metric", transform + strings.ReplaceAll(modify, "50.0", "+Inf")},
		{"wrong_units", transform + strings.ReplaceAll(modify, "ns/op", "ms/op")},
		{"unknown_variant", transform + strings.ReplaceAll(modify, "modify", "unknown")},
		{"invalid_cpu", transform + strings.ReplaceAll(modify, "-8", "-invalid")},
		{"invalid_iterations", transform + strings.ReplaceAll(modify, "1000", "invalid")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := formatResults(tc.input)
			require.Error(t, err)
			require.Equal(t, tc.input, output, "preserve diagnostics on failure")
		})
	}
}
