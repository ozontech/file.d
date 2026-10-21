# Transform benchmarks

Run from the repository root:

```sh
make bench-transform
make bench-transform BENCH_ARGS='-count=5 -benchtime=1s -cpu=8'
make bench-transform BENCH_ARGS='-count=2 -benchtime=257x -cpu=1,8'
```

Defaults are `-count=1 -benchtime=1s -cpu=8`. Results go only to the
terminal. The runner keeps raw output in memory and formats it with Go.

## Coverage

| Group | Scenarios | Workloads |
| --- | ---: | --- |
| One plugin vs transform | 55 pairs | Assignments, copies, string composition, trim/trim_to/cut, regex, log parsing, and rename |
| N modify vs transform | 30 pairs | Value mapping, bounded tag extraction, and dependent conditional actions |
| Transform-only | 12 | Dynamic function arguments, conditional collection aggregation, and typed tag extraction |

One repetition with one CPU setting produces 182 standard Go benchmark rows.
Paired scenarios run first, followed by transform-only scenarios. Declaration
order determines scenario order. Each paired result places transform before
its comparator (`modify`, `rename`, or `modify_xN`) and then shows percentage
differences for `ns/op`, `B/op`, and `allocs/op`.

Percentages use `(comparator - transform) / transform * 100`. Negative means
lower cost for the comparator. Percentages precede units; two zeros yield
`0.0%`, and a zero transform baseline with a positive comparator yields `n/a`.
Transform-only results have no comparison or percentages.

## What one operation measures

One operation transforms one already decoded event through prepared plugins.
It includes `Do` and the real `do_if` checks needed by conditional modify
actions. A chain measures the complete sequence per event, without dividing
by its number of plugins.

Configuration, compilation, `Start`, warmup, JSON decoding, fixture creation,
event restoration, validation, and report formatting are excluded from all
three metrics. Events start with their original content on every operation.
Execution is sequential: `-cpu=8` sets GOMAXPROCS, not eight workers. Full
pipeline I/O and scheduling are outside this benchmark.

Go chooses the iteration count unless `-benchtime=Nx` fixes it. Containers are
reused in batches of at most 256; this is neither the number of unique inputs
nor the total operation count. Mixed inputs cycle in declaration order across
batch boundaries, including the final partial cycle.

## Input domains

Paired solutions produce identical complete events within their declared
domains, including types and unrelated fields. Regex is used by both sides
of a pair or by neither.

- Paired cut/slice workloads use ASCII because modify counts bytes and
  transform counts Unicode code points.
- Rename removes the old field and preserves its JSON type. Guarded rename
  inputs exclude explicit null sources and, in preserve mode, null
  destinations; nested null values remain valid.
- Log parsing uses a fixed grammar documented in
  [logs_fixtures_test.go](logs_fixtures_test.go). Regex and regex-free
  comparisons are separate. `full_regex` is not a continuation of the old
  mixed-algorithm `full` baseline.
- Mapping uses disjoint codes and replacement names to prevent cascading
  conditional replacements. Arbitrary cyclic mappings are outside its domain.
- Paired tags contain at most N string pairs, or are missing/null. N is the
  configured modify action count. The last match wins; no match preserves the
  destination.
- Transform-only workloads use event-dependent trim/slice arguments,
  aggregation of variable-length collections, and typed tag values. Tag
  arrays extend to 256 entries, beyond the paired limit of 64. The original
  tag program has no length limit.

## Mixed corpora

Each row below describes one complete cycle. The mixtures are synthetic,
not production traffic statistics. Homogeneous profiles remain available.

| Workload | Ordered mixture |
| --- | --- |
| Log parsing | 128 B ASCII, 4096 B ASCII, 512 B Unicode; 1/3 each |
| Mapping | First match, last match, unknown code; 1/3 each |
| Paired tags | Full first/last/no match, short last match, empty, repeated matches ending in an empty string; 1/6 each |
| Dependent actions | Matching service at 128 B and 4096 B, nonmatching service at 512 B; 1/3 each |
| Dynamic trim/slice | 128 B, 4096 B, negative start with null end, Unicode with negative end; 1/4 each |
| Collection aggregation | 4, 64, 256, and zero items; 1/4 of events each |
| Typed tags | 4/integer, 256/integer, 4/object, 256/object, 4/bool, 4/float, 4/null, 256/array, 256/no match, empty; 1/10 each |

Different Go iteration counts can end at different positions in the same
corpus. Use a fixed count when comparing identical finite input sequences.
Keep fixtures, Go version, hardware, and flags constant across revisions.
Use repeated measurements before drawing conclusions from small differences.

## Validation and extension

```sh
go test ./benchmarks/transform/...
go test -race ./benchmarks/transform/...
```

Fixtures specify expected complete events independently of plugin execution.
Shared checks exercise real plugins, alternating inputs and reusing event
containers. Execution errors invalidate a benchmark even if the plugin returns
`ActionPass`. Each variant starts with fresh decoder storage before warmup to
isolate storage retained by the shared JSON pool from earlier workloads.

Add paired scenarios to `transformCallScenarios` or standalone scenarios to
`transformOnlyScenarios`. Use `inputs` for measured events and `cases` for
additional correctness checks. Keep transform programs inline in `source`,
use named struct types, and place each struct field on its own line. Shared
checks and reporting must not branch on scenario names.
