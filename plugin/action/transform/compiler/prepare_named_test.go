package compiler

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrepareNamedAndCollectionCalls(t *testing.T) {
	tests := []struct {
		name   string
		source string
		input  string
		want   string
		err    string
	}{
		{
			name: "slice defaults and dynamic bounds",
			source: `
				.omitted = slice(.value, .start)
				.explicit_null = slice(.value, .start, end: null)
				.bounded = slice(.value, .start, end: .end)
			`,
			input: `{"value":"é界🎯end","start":1,"end":-1}`,
			want:  `{"value":"é界🎯end","start":1,"end":-1,"omitted":"界🎯end","explicit_null":"界🎯end","bounded":"界🎯en"}`,
		},
		{
			name: "slice clamps bounds",
			source: `
				.whole = slice("abc", -100, end: 100)
				.past = slice("abc", 100)
				.reversed = slice("abc", 2, end: 1)
				.before = slice("abc", 0, end: -100)
				.empty = slice("", 0)
			`,
			input: `{}`,
			want:  `{"whole":"abc","past":"","reversed":"","before":"","empty":""}`,
		},
		{
			name: "lookup omitted default and explicit null",
			source: `
				.hit = lookup(.value, .table, default: "unused")
				.omitted = lookup(.miss, .table)
				.explicit_null = lookup(.miss, .table, default: null)
				.dynamic = lookup(.miss, .table, default: .fallback)
			`,
			input: `{"value":1,"miss":7,"table":{"1":"one"},"fallback":["unknown"]}`,
			want:  `{"value":1,"miss":7,"table":{"1":"one"},"fallback":["unknown"],"hit":"one","omitted":7,"explicit_null":null,"dynamic":["unknown"]}`,
		},
		{
			name: "lookup null entries and scalar keys",
			source: `
				.missing = lookup(.missing, {})
				.explicit_null = lookup("key", {key: null}, default: "unused")
				.bool = lookup(true, {"true": "yes"})
				.float = lookup(1.5, {"1.5": "fraction"})
				.null_key = lookup(null, {"null": "found"})
			`,
			input: `{}`,
			want:  `{"missing":null,"explicit_null":null,"bool":"yes","float":"fraction","null_key":"found"}`,
		},
		{
			name: "lookup returns shared collections without mutating them",
			source: `
				table = {key: ["original"]}
				items = lookup("key", table)
				items[0] = "changed"
				.original = lookup("key", table)
				.changed = items
				fallback = {state: "original"}
				obj = lookup("missing", table, default: fallback)
				obj["state"] = "changed"
				.fallback = lookup("missing", table, default: fallback)
				.object = obj
			`,
			input: `{}`,
			want:  `{"original":["original"],"changed":["changed"],"fallback":{"state":"original"},"object":{"state":"changed"}}`,
		},
		{
			name: "regex defaults and dynamic options",
			source: `
				pattern = r'(?P<word>\p{L}+)(\d+)?'
				.named = parse_regex(.value, pattern)
				.numeric = parse_regex(.value, pattern, numeric_groups: .include_numeric)
				.miss = parse_regex("", pattern)
				.unnamed = parse_regex("1", r'(\d)')
			`,
			input: `{"value":"界42","include_numeric":true}`,
			want:  `{"value":"界42","include_numeric":true,"named":{"word":"界"},"numeric":{"word":"界","0":"界42","1":"界","2":"42"},"miss":null,"unnamed":{}}`,
		},
		{
			name: "regex_all defaults and named argument order",
			source: `
				pattern = r're(\d+)'
				.all = parse_regex_all(.value, pattern)
				.forward = parse_regex_all(.value, pattern, group: .group, limit: .limit)
				.reverse = parse_regex_all(.value, pattern, limit: .limit, group: .group)
				.negative = parse_regex_all(.value, pattern, limit: -5)
				.zero = parse_regex_all(.value, pattern, limit: 0)
			`,
			input: `{"value":"re1 re2 re3","group":1,"limit":2}`,
			want:  `{"value":"re1 re2 re3","group":1,"limit":2,"all":["re1","re2","re3"],"forward":["1","2"],"reverse":["1","2"],"negative":["re1","re2","re3"],"zero":[]}`,
		},
		{
			name: "regex empty matches and optional groups",
			source: `
				.optional = parse_regex("b", r'(?P<a>a)?b', numeric_groups: true)
				.skipped = parse_regex_all("a b", r'(a)|(b)', group: 1)
				.empty_group = parse_regex_all("b", r'(a*)b', group: 1)
				.empty = parse_regex_all("", r'')
				.miss = parse_regex_all("", r'x')
			`,
			input: `{}`,
			want:  `{"optional":{"a":"","0":"b","1":""},"skipped":["a"],"empty_group":[""],"empty":[""],"miss":[]}`,
		},
		{
			name: "join collections and dynamic separator",
			source: `
				.joined = join(.items, .separator)
				.empty = join([], ",")
				.single = join(["only"], ",")
				.no_separator = join(["a", "b"], "")
				.nested = join(lookup("key", {key: ["a", "b"]}), "|")
			`,
			input: `{"items":["é","","界"],"separator":"🎯"}`,
			want:  `{"items":["é","","界"],"separator":"🎯","joined":"é🎯🎯界","empty":"","single":"only","no_separator":"ab","nested":"a|b"}`,
		},
		{
			name: "to_string accepts every value kind",
			source: `
				.out = [to_string(.missing), to_string(null), to_string(true),
					to_string(false), to_string(42), to_string(1.5), to_string("é"),
					to_string(["a", 1]), to_string({key: "value"}),
					to_string(r'a+'), to_string(t'2026-01-01T00:00:00Z')]
			`,
			input: `{}`,
			want:  `{"out":["","","true","false","42","1.5","é","[\"a\", 1]","{\"key\": \"value\"}","r'a+'","2026-01-01T00:00:00Z"]}`,
		},
		{
			name: "collections in conditions loops and short circuit",
			source: `
				if parse_regex(.value, r'\d') != null {
					for i, item in parse_regex_all(.value, r'\d+') {
						.out[i] = join([slice(item, 0, end: 1), to_string(lookup(item, {"12": "first"}))], ":")
					}
				} else {
					.out = join([null], ",")
				}
				.skip = false && parse_regex_all(null, r'.') == []
				.keep = true || lookup([], {}) == null
			`,
			input: `{"value":"12 34","out":["",""]}`,
			want:  `{"value":"12 34","out":["1:first","3:34"],"skip":false,"keep":true}`,
		},
		{
			name:   "slice missing input",
			source: `.out = slice(.missing, 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "slice": argument "value": expected string, got null`,
		},
		{
			name:   "slice invalid start",
			source: `.out = slice("abc", 1.5)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "slice": argument "start": expected integer, got float`,
		},
		{
			name:   "slice invalid end",
			source: `.out = slice("abc", 0, end: false)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "slice": argument "end": expected integer or null, got bool`,
		},
		{
			name:   "lookup invalid key and table checks key first",
			source: `.out = lookup([], null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "lookup": argument "value": expected null or bool or integer or float or string, got array`,
		},
		{
			name:   "lookup invalid table",
			source: `.out = lookup("key", .missing)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "lookup": argument "table": expected object, got null`,
		},
		{
			name:   "lookup evaluates default even on hit",
			source: `.out = lookup("key", {key: "found"}, default: 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "regex requires compiled pattern",
			source: `.out = parse_regex("abc", "a")`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "parse_regex": argument "pattern": expected regex, got string`,
		},
		{
			name:   "regex null option",
			source: `.out = parse_regex("abc", r'a', numeric_groups: null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "parse_regex": argument "numeric_groups": expected bool, got null`,
		},
		{
			name:   "regex_all missing input",
			source: `.out = parse_regex_all(.missing, r'a')`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "parse_regex_all": argument "value": expected string, got null`,
		},
		{
			name:   "named type checks follow parameter order",
			source: `.out = parse_regex_all("a", r'a', limit: false, group: null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "parse_regex_all": argument "group": expected integer, got null`,
		},
		{
			name:   "regex_all null limit",
			source: `.out = parse_regex_all("a", r'a', limit: null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "parse_regex_all": argument "limit": expected integer, got null`,
		},
		{
			name:   "named evaluation errors follow source order",
			source: `.out = parse_regex_all("a", r'a', limit: 1 / 0, group: join([1], ","))`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "reversed named evaluation errors",
			source: `.out = parse_regex_all("a", r'a', group: join([1], ","), limit: 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "join: element 0: expected string, got integer",
		},
		{
			name:   "named evaluation error precedes positional type check",
			source: `.out = parse_regex_all(null, r'a', limit: 1 / 0)`,
			input:  `{}`,
			want:   `{}`,
			err:    "division by zero",
		},
		{
			name:   "regex_all negative group",
			source: `.out = parse_regex_all("a", r'(a)', group: -1)`,
			input:  `{}`,
			want:   `{}`,
			err:    "parse_regex_all: group -1 is out of range: pattern has 1 capture groups",
		},
		{
			name: "partial writes before regex operation error",
			source: `
				.saved = join(["a", "b"], ",")
				.out = parse_regex_all("a", r'(a)', group: 2, limit: 0)
				.after = "unreachable"
			`,
			input: `{}`,
			want:  `{"saved":"a,b"}`,
			err:   "parse_regex_all: group 2 is out of range: pattern has 1 capture groups",
		},
		{
			name:   "join missing input",
			source: `.out = join(.missing, ",")`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "join": argument "value": expected array, got null`,
		},
		{
			name:   "join invalid separator precedes element check",
			source: `.out = join([1], null)`,
			input:  `{}`,
			want:   `{}`,
			err:    `function "join": argument "separator": expected string, got null`,
		},
		{
			name:   "join invalid element",
			source: `.out = join(["a", null], ",")`,
			input:  `{}`,
			want:   `{}`,
			err:    "join: element 1: expected string, got null",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checkPreparedCalls(t, tt.source, tt.input, tt.want, tt.err)
		})
	}
}

func TestPreparedCollectionsConcurrentEvents(t *testing.T) {
	exprs := compileCalls(t, `
		table = {key: ["original"]}
		items = lookup("key", table)
		items[0] = to_string(.value)
		.changed = join(items, ",")
		.original = lookup("key", table)
		fallback = {state: "original"}
		obj = lookup("missing", table, default: fallback)
		obj["state"] = to_string(.value)
		.object = obj
		.fallback = lookup("missing", table, default: fallback)
		.unchanged = lookup(.value, table)
		.numeric = parse_regex(.value, r'(?P<id>\d+)', numeric_groups: true)
		.named = parse_regex(.value, r'(?P<id>\d+)')
		.parts = parse_regex_all(.value, r'(\d+)', limit: 1, group: 1)
		.all = parse_regex_all(.value, r'\d+')
		.out = to_string(slice(join(.all, ":"), 0))
	`, true)
	for i := range 16 {
		t.Run(fmt.Sprintf("worker_%d", i), func(t *testing.T) {
			t.Parallel()

			for j := range 20 {
				input := fmt.Sprintf(`{"value":"%d:%d"}`, i, j)
				_, event, err := runCalls(t, exprs, input)
				require.NoError(t, err)
				want := fmt.Sprintf(`{
					"value":"%[1]d:%[2]d", "changed":"%[1]d:%[2]d", "original":["original"],
					"object":{"state":"%[1]d:%[2]d"}, "fallback":{"state":"original"}, "unchanged":"%[1]d:%[2]d",
					"numeric":{"id":"%[1]d","0":"%[1]d","1":"%[1]d"}, "named":{"id":"%[1]d"},
					"parts":["%[1]d"], "all":["%[1]d","%[2]d"], "out":"%[1]d:%[2]d"
				}`, i, j)
				require.JSONEq(t, want, event)
			}
		})
	}
}
