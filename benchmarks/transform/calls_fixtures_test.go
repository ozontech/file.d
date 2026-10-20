package transformbench

import "github.com/ozontech/file.d/plugin/action/modify"

// Expected values are fixed independently of either plugin. Slice order is
// also the execution and output order of the paired benchmarks.
func transformCallScenarios() []transformCallScenario {
	return []transformCallScenario{
		{
			name: "assign_new5",
			inputs: []transformCallCase{
				{
					name:  "default",
					input: `{"source":"x"}`,
					want:  `{"source":"x","a":"v1","b":"v2","c":"v3","d":"v4","e":"v5"}`,
				},
			},
			source: `
				.a = "v1"
				.b = "v2"
				.c = "v3"
				.d = "v4"
				.e = "v5"
			`,
			actions: []transformCallAction{
				{config: &modify.Config{"a": "v1", "b": "v2", "c": "v3", "d": "v4", "e": "v5"}},
			},
		},
		{
			name: "assign_existing5",
			inputs: []transformCallCase{
				{
					name:  "default",
					input: `{"source":"x","a":"old","b":"old","c":"old","d":"old","e":"old"}`,
					want:  `{"source":"x","a":"v1","b":"v2","c":"v3","d":"v4","e":"v5"}`,
				},
			},
			source: `
				.a = "v1"
				.b = "v2"
				.c = "v3"
				.d = "v4"
				.e = "v5"
			`,
			actions: []transformCallAction{
				{config: &modify.Config{"a": "v1", "b": "v2", "c": "v3", "d": "v4", "e": "v5"}},
			},
		},
		{
			name: "trim_one",
			inputs: []transformCallCase{
				{
					name:  "default",
					input: `{"source":"api123"}`,
					want:  `{"source":"api123","out":"api"}`,
				},
			},
			source: `.out = trim_right(.source, "0123456789")`,
			actions: []transformCallAction{
				{config: &modify.Config{"out": `${source|trim("right","0123456789")}`}},
			},
			cases: []transformCallCase{
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","out":""}`,
				},
				{
					name:  "preserve_extra_fields",
					input: `{"source":"api123","count":2,"enabled":true,"tags":["edge"],"meta":{"zone":"a"}}`,
					want:  `{"source":"api123","out":"api","count":2,"enabled":true,"tags":["edge"],"meta":{"zone":"a"}}`,
				},
			},
		},
		{
			name: "trim4",
			inputs: []transformCallCase{
				{
					name:  "default",
					input: `{"source":"api-123z45"}`,
					want:  `{"source":"api-123z45","cluster_name":"api"}`,
				},
			},
			source: `
				host = trim_right(.source, "0123456789")
				.cluster_name = trim_right(
					trim_right(
						trim_right(host, "z"),
						"0123456789"
					),
					"-"
				)
			`,
			actions: []transformCallAction{
				{config: &modify.Config{
					"cluster_name": `${source` +
						`|trim("right","0123456789")` +
						`|trim("right","z")` +
						`|trim("right","0123456789")` +
						`|trim("right","-")}`,
				}},
			},
			cases: []transformCallCase{
				{
					name:  "domain",
					input: `{"source":"api-123z45.example.net"}`,
					want:  `{"source":"api-123z45.example.net","cluster_name":"api-123z45.example.net"}`,
				},
				{
					name:  "multiple_dots",
					input: `{"source":"api-123z45.a.b"}`,
					want:  `{"source":"api-123z45.a.b","cluster_name":"api-123z45.a.b"}`,
				},
				{
					name:  "no_dot",
					input: `{"source":"api-123z45"}`,
					want:  `{"source":"api-123z45","cluster_name":"api"}`,
				},
				{
					name:  "leading_dot",
					input: `{"source":".domain"}`,
					want:  `{"source":".domain","cluster_name":".domain"}`,
				},
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","cluster_name":""}`,
				},
				{
					name:  "newline",
					input: `{"source":"api\nnode-123z45.domain"}`,
					want:  `{"source":"api\nnode-123z45.domain","cluster_name":"api\nnode-123z45.domain"}`,
				},
				{
					name:  "z_before_digits",
					input: `{"source":"api-z123.domain"}`,
					want:  `{"source":"api-z123.domain","cluster_name":"api-z123.domain"}`,
				},
				{
					name:  "only_digits",
					input: `{"source":"123"}`,
					want:  `{"source":"123","cluster_name":""}`,
				},
				{
					name:  "multiple_dashes",
					input: `{"source":"api--123z45.domain"}`,
					want:  `{"source":"api--123z45.domain","cluster_name":"api--123z45.domain"}`,
				},
			},
		},
		{
			name: "full",
			inputs: []transformCallCase{
				{
					name:  "default",
					input: `{"source":"api-123z45.example.net"}`,
					want:  `{"source":"api-123z45.example.net","cluster_name":"api"}`,
				},
			},
			source: `
				host = trim_right(
					before(.source, "."),
					"0123456789"
				)
				.cluster_name = trim_right(
					trim_right(
						trim_right(host, "z"),
						"0123456789"
					),
					"-"
				)
			`,
			actions: []transformCallAction{
				{config: &modify.Config{
					"cluster_name": `${source` +
						`|re("^([^.]*)",1,[1],"")` +
						`|trim("right","0123456789")` +
						`|trim("right","z")` +
						`|trim("right","0123456789")` +
						`|trim("right","-")}`,
				}},
			},
			cases: []transformCallCase{
				{
					name:  "domain",
					input: `{"source":"api-123z45.example.net"}`,
					want:  `{"source":"api-123z45.example.net","cluster_name":"api"}`,
				},
				{
					name:  "multiple_dots",
					input: `{"source":"api-123z45.a.b"}`,
					want:  `{"source":"api-123z45.a.b","cluster_name":"api"}`,
				},
				{
					name:  "no_dot",
					input: `{"source":"api-123z45"}`,
					want:  `{"source":"api-123z45","cluster_name":"api"}`,
				},
				{
					name:  "leading_dot",
					input: `{"source":".domain"}`,
					want:  `{"source":".domain","cluster_name":""}`,
				},
				{
					name:  "empty",
					input: `{"source":""}`,
					want:  `{"source":"","cluster_name":""}`,
				},
				{
					name:  "newline",
					input: `{"source":"api\nnode-123z45.domain"}`,
					want:  `{"source":"api\nnode-123z45.domain","cluster_name":"api\nnode"}`,
				},
				{
					name:  "z_before_digits",
					input: `{"source":"api-z123.domain"}`,
					want:  `{"source":"api-z123.domain","cluster_name":"api"}`,
				},
				{
					name:  "only_digits",
					input: `{"source":"123"}`,
					want:  `{"source":"123","cluster_name":""}`,
				},
				{
					name:  "multiple_dashes",
					input: `{"source":"api--123z45.domain"}`,
					want:  `{"source":"api--123z45.domain","cluster_name":"api"}`,
				},
			},
		},
	}
}
