package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	callsBenchmark   = "BenchmarkTransformCalls"
	onlyBenchmark    = "BenchmarkTransformOnly"
	transformVariant = "transform"
	modifyVariant    = "modify"
)

type result struct {
	fields     []string
	metrics    [3]float64
	key        string
	variant    string
	standalone bool
}

type resultGroup struct {
	transform  []result
	comparator []result
}

type reportEntry struct {
	text  string
	group *resultGroup
}

// formatResults keeps scenario/CPU order from the benchmark, pairing each
// repetition even if the variants arrive separately. Explicit standalone
// benchmarks are never inferred from a missing comparator.
func formatResults(output string) (string, error) {
	groups := make(map[string]*resultGroup)
	entries := make([]reportEntry, 0)
	widths := [5]int{64, 10, 12, 10, 10}
	for line := range strings.SplitAfterSeq(output, "\n") {
		if !strings.HasPrefix(line, callsBenchmark) && !strings.HasPrefix(line, onlyBenchmark) {
			entries = append(entries, reportEntry{text: line})
			continue
		}
		current, err := parseResult(line)
		if err != nil {
			return output, err
		}
		for i, field := range []int{0, 1, 2, 4, 6} {
			widths[i] = max(widths[i], len(current.fields[field]))
		}
		group, ok := groups[current.key]
		if !ok {
			group = &resultGroup{}
			groups[current.key] = group
			entries = append(entries, reportEntry{group: group})
		}
		if current.variant == transformVariant {
			group.transform = append(group.transform, current)
		} else {
			if len(group.comparator) > 0 && group.comparator[0].variant != current.variant {
				return output, fmt.Errorf("multiple comparators for %s", current.key)
			}
			group.comparator = append(group.comparator, current)
		}
	}
	if len(groups) == 0 {
		return output, errors.New("no benchmark results")
	}

	var report strings.Builder
	for _, entry := range entries {
		if entry.group == nil {
			report.WriteString(entry.text)
			continue
		}
		group := entry.group
		if len(group.transform) == 0 {
			return output, fmt.Errorf("missing transform result for %s", group.comparator[0].key)
		}
		if !group.transform[0].standalone && len(group.transform) != len(group.comparator) {
			return output, fmt.Errorf("incomplete comparison for %s", group.transform[0].key)
		}
		for i, base := range group.transform {
			base.writeTo(&report, widths)
			if base.standalone {
				continue
			}
			comparator := group.comparator[i]
			comparator.writeTo(&report, widths)
			fmt.Fprintf(&report, "%s vs transform: %s ns/op, %s B/op, %s allocs/op\n\n",
				comparator.variant,
				percent(base.metrics[0], comparator.metrics[0]),
				percent(base.metrics[1], comparator.metrics[1]),
				percent(base.metrics[2], comparator.metrics[2]))
		}
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
	if len(parts) < 3 || (parts[0] != callsBenchmark && parts[0] != onlyBenchmark) {
		return result{}, invalid
	}
	for _, part := range parts[1 : len(parts)-1] {
		if part == "" {
			return result{}, invalid
		}
	}
	variant, cpu, hasCPU := strings.Cut(parts[len(parts)-1], "-")
	standalone := parts[0] == onlyBenchmark
	if !validVariant(variant) || (standalone && variant != transformVariant) {
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

	r := result{
		fields:     fields,
		key:        strings.Join(parts[:len(parts)-1], "/") + "/" + cpu,
		variant:    variant,
		standalone: standalone,
	}
	for i := range r.metrics {
		value, err := strconv.ParseFloat(fields[2+i*2], 64)
		if err != nil || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return result{}, invalid
		}
		r.metrics[i] = value
	}
	return r, nil
}

func validVariant(variant string) bool {
	switch variant {
	case transformVariant, modifyVariant, "rename":
		return true
	}
	count, ok := strings.CutPrefix(variant, "modify_x")
	if !ok {
		return false
	}
	n, err := strconv.ParseUint(count, 10, 64)
	return err == nil && n > 1 && strconv.FormatUint(n, 10) == count
}

func (r *result) writeTo(out *strings.Builder, widths [5]int) {
	f := r.fields
	fmt.Fprintf(out, "%-*s %*s %*s %s %*s %s %*s %s\n",
		widths[0], f[0], widths[1], f[1], widths[2], f[2], f[3],
		widths[3], f[4], f[5], widths[4], f[6], f[7])
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
