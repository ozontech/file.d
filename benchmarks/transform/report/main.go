// Command report runs selected paired and standalone transform benchmarks.
// Run from the repository root; arguments after -- are passed to go test.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil && !errors.Is(err, flag.ErrHelp) {
		_, _ = fmt.Fprintln(os.Stderr, "transform benchmark:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	command, list, err := benchmarkCommand(args, out)
	if err != nil {
		return err
	}

	output, err := exec.Command("go", command...).CombinedOutput()
	if err != nil {
		_, writeErr := out.Write(output)
		return errors.Join(err, writeErr)
	}
	if list {
		listing, listErr := formatCaseList(string(output))
		_, writeErr := io.WriteString(out, listing)
		return errors.Join(listErr, writeErr)
	}
	formatted, formatErr := formatResults(string(output))
	_, writeErr := io.WriteString(out, formatted)
	return errors.Join(formatErr, writeErr)
}

func benchmarkCommand(args []string, out io.Writer) ([]string, bool, error) {
	flags := flag.NewFlagSet("bench-transform", flag.ContinueOnError)
	flags.SetOutput(out)
	group := flags.String("group", "smoke", "benchmark group: smoke, single, chains, only, all")
	prefix := flags.String("case", "", "scenario path or prefix within the group")
	list := flags.Bool("list", false, "list selected CASE paths without running benchmarks")
	if err := flags.Parse(args); err != nil {
		return nil, false, err
	}
	// Selection belongs to GROUP/CASE. A second Go benchmark filter could
	// remove one half of a pair; listing/verbose/JSON modes bypass the report.
	for _, arg := range flags.Args() {
		if !strings.HasPrefix(arg, "-") {
			continue
		}
		name, _, _ := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		name = strings.TrimPrefix(name, "test.")
		switch name {
		case "bench", "run", "list", "json", "v", "args", "":
			return nil, false, fmt.Errorf("%s is not supported in BENCH_ARGS; use GROUP, CASE and LIST for selection", arg)
		}
		if strings.HasPrefix(name, "transform.") {
			return nil, false, fmt.Errorf("%s is reserved; use GROUP, CASE and LIST", arg)
		}
	}
	command := []string{
		"test", "./benchmarks/transform", "-run=^$", "-bench=^BenchmarkTransform(Calls|Only)$",
		"-benchmem", "-count=1", "-benchtime=1s", "-cpu=8",
	}
	command = append(command, flags.Args()...)
	command = append(command, "-transform.group="+*group, "-transform.case="+*prefix,
		fmt.Sprintf("-transform.list=%t", *list))
	return command, *list, nil
}

func formatCaseList(output string) (string, error) {
	var listing strings.Builder
	for line := range strings.SplitSeq(output, "\n") {
		if path, ok := strings.CutPrefix(line, "CASE "); ok && path != "" {
			listing.WriteString(path + "\n")
		}
	}
	if listing.Len() == 0 {
		return output, errors.New("no benchmark scenarios listed")
	}
	return listing.String(), nil
}
