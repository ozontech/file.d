package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

type result struct {
	fields  []string
	metrics [3]float64
	key     string
	variant string
}

// formatResults pairs repetitions at the same CPU count. On invalid or
// incomplete results it returns the original output with an error.
func formatResults(output string) (string, error) {
	var report strings.Builder
	pending := make(map[string][]result)
	pairs := 0
	for line := range strings.SplitAfterSeq(output, "\n") {
		if !strings.HasPrefix(line, "BenchmarkTransformCalls/") {
			report.WriteString(line)
			continue
		}
		current, err := parseResult(line)
		if err != nil {
			return output, err
		}
		if current.variant == "transform" {
			pending[current.key] = append(pending[current.key], current)
			continue
		}
		queue := pending[current.key]
		if len(queue) == 0 {
			return output, fmt.Errorf("missing transform result for %s", current.fields[0])
		}
		base := queue[0]
		if len(queue) == 1 {
			delete(pending, current.key)
		} else {
			pending[current.key] = queue[1:]
		}
		base.writeTo(&report)
		current.writeTo(&report)
		fmt.Fprintf(&report, "modify vs transform: %s ns/op, %s B/op, %s allocs/op\n\n",
			percent(base.metrics[0], current.metrics[0]),
			percent(base.metrics[1], current.metrics[1]),
			percent(base.metrics[2], current.metrics[2]))
		pairs++
	}
	if len(pending) != 0 {
		return output, errors.New("missing modify results")
	}
	if pairs == 0 {
		return output, errors.New("no paired benchmark results")
	}
	return report.String(), nil
}

func parseResult(line string) (result, error) {
	fields := strings.Fields(line)
	invalid := fmt.Errorf("unexpected result: %s", strings.TrimSpace(line))
	if len(fields) != 8 || fields[3] != "ns/op" || fields[5] != "B/op" || fields[7] != "allocs/op" {
		return result{}, invalid
	}
	parts := strings.Split(fields[0], "/")
	if len(parts) != 3 || parts[1] == "" {
		return result{}, invalid
	}
	variant, cpu, hasCPU := strings.Cut(parts[2], "-")
	if variant != "transform" && variant != "modify" {
		return result{}, invalid
	}
	if hasCPU {
		if n, err := strconv.ParseUint(cpu, 10, 64); err != nil || n == 0 {
			return result{}, invalid
		}
	}
	if n, err := strconv.ParseUint(fields[1], 10, 64); err != nil || n == 0 {
		return result{}, invalid
	}
	r := result{fields: fields, key: parts[1] + "/" + cpu, variant: variant}
	for i := range r.metrics {
		value, err := strconv.ParseFloat(fields[2+i*2], 64)
		if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return result{}, invalid
		}
		r.metrics[i] = value
	}
	return r, nil
}

func (r *result) writeTo(out *strings.Builder) {
	f := r.fields
	fmt.Fprintf(out, "%-64s %10s %12s %s %10s %s %10s %s\n",
		f[0], f[1], f[2], f[3], f[4], f[5], f[6], f[7])
}

func percent(base, current float64) string {
	if base == 0 {
		if current == 0 {
			return "0.0%"
		}
		return "n/a"
	}
	delta := (current - base) / base * 100
	if math.Round(delta*10) == 0 {
		return "0.0%"
	}
	return fmt.Sprintf("%+.1f%%", delta)
}
