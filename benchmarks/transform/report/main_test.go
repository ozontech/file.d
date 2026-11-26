package main

import (
	"flag"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransformBenchmarkCommand(t *testing.T) {
	type testCase struct {
		name      string
		args      []string
		group     string
		prefix    string
		benchtime string
		list      bool
		extra     []string
	}
	tests := []testCase{
		{
			name:      "representative_default",
			group:     "smoke",
			benchtime: "1s",
		},
		{
			name:      "full_suite",
			args:      []string{"-group=all"},
			group:     "all",
			benchtime: "1s",
		},
		{
			name:      "single_case",
			args:      []string{"-group=single", "-case=strings/trim/right/128B"},
			group:     "single",
			prefix:    "strings/trim/right/128B",
			benchtime: "1s",
		},
		{
			name:      "list_chains",
			args:      []string{"-group=chains", "-case=mapping/16", "-list=1"},
			group:     "chains",
			prefix:    "mapping/16",
			benchtime: "1s",
			list:      true,
		},
		{
			name:      "standalone_with_go_arguments",
			args:      []string{"-group=only", "--", "-benchtime=257x", "-count", "2", "-cpu=1,8"},
			group:     "only",
			benchtime: "1s",
			extra:     []string{"-benchtime=257x", "-count", "2", "-cpu=1,8"},
		},
		{
			name:      "override_smoke_duration",
			args:      []string{"--", "-benchtime=3s"},
			group:     "smoke",
			benchtime: "1s",
			extra:     []string{"-benchtime=3s"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			command, list, err := benchmarkCommand(tc.args, io.Discard)
			require.NoError(t, err)
			require.Equal(t, tc.list, list)
			require.Equal(t, []string{"test", "./benchmarks/transform"}, command[:2])
			require.Contains(t, command, "-run=^$")
			require.Contains(t, command, "-bench=^BenchmarkTransform(Calls|Only)$")
			require.Contains(t, command, "-benchmem")
			require.Contains(t, command, "-count=1")
			require.Contains(t, command, "-benchtime="+tc.benchtime)
			require.Contains(t, command, "-cpu=8")
			require.Contains(t, command, "-transform.group="+tc.group)
			require.Contains(t, command, "-transform.case="+tc.prefix)
			if len(tc.extra) != 0 {
				// Explicit Go arguments follow defaults, so they take precedence.
				require.Equal(t, tc.extra, command[8:8+len(tc.extra)])
			}
		})
	}
}

func TestTransformBenchmarkCommandRejectsConflictingArguments(t *testing.T) {
	arguments := []string{
		"-bench=Calls/trim4/modify",
		"-test.bench=Calls",
		"--bench=Calls",
		"-run=TestTransformBenchmarkScenarios",
		"-list=.",
		"-json",
		"-v",
		"-args",
		"-transform.group=all",
		"-test.transform.case=trim4",
		"--",
	}
	for _, arg := range arguments {
		t.Run(arg, func(t *testing.T) {
			_, _, err := benchmarkCommand([]string{"--", arg}, io.Discard)
			require.ErrorContains(t, err, "GROUP")
		})
	}
}

func TestTransformBenchmarkCommandHelp(t *testing.T) {
	var output strings.Builder
	require.ErrorIs(t, run([]string{"-help"}, &output), flag.ErrHelp)
	require.Contains(t, output.String(), "benchmark group")
}

func TestTransformBenchmarkCaseList(t *testing.T) {
	type testCase struct {
		name   string
		output string
		want   string
		err    bool
	}
	tests := []testCase{
		{
			name:   "paths_without_startup_or_go_output",
			output: "logger initialized\nCASE mapping/16/first\nCASE mapping/16/mixed\nok package 0.1s\n",
			want:   "mapping/16/first\nmapping/16/mixed\n",
		},
		{
			name:   "no_paths_preserves_diagnostics",
			output: "unexpected output\n",
			want:   "unexpected output\n",
			err:    true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := formatCaseList(tc.output)
			if tc.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, got)
		})
	}
}
