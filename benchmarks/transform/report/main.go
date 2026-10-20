// Command report runs paired and standalone transform benchmarks.
// Run from the repository root; extra arguments are passed to go test.
package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "transform benchmark:", err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	command := []string{
		"test", "./benchmarks/transform", "-run=^$", "-bench=^BenchmarkTransform(Calls|Only)$",
		"-benchmem", "-count=1", "-benchtime=1s", "-cpu=8",
	}

	output, err := exec.Command("go", append(command, args...)...).CombinedOutput()
	if err != nil {
		_, writeErr := out.Write(output)
		return errors.Join(err, writeErr)
	}
	formatted, formatErr := formatResults(string(output))
	_, writeErr := io.WriteString(out, formatted)
	return errors.Join(formatErr, writeErr)
}
