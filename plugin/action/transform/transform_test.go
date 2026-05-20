package transform

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type eventCase struct {
	in     string
	fields map[string]string
}

func TestLanguage(t *testing.T) {
	tests := []struct {
		name   string
		source string
		events []eventCase
	}{
		{
			name:   "assign",
			source: `.res = "hello"`,
			events: []eventCase{
				{
					in:     `{"x":1}`,
					fields: map[string]string{"res": "hello"},
				},
			},
		},
		{
			name: "literals",
			source: `
			.str   = "hello"
			.raw   = s'no\escape'
			.num   = 42
			.flt   = 3.14
			.bool  = true
			.nl    = null
		`,
			events: []eventCase{
				{
					in: `{"x":1}`,
					fields: map[string]string{
						"str":  "hello",
						"raw":  `no\escape`,
						"num":  "42",
						"flt":  "3.14",
						"bool": "true",
						"nl":   "null",
					},
				},
			},
		}, {
			name: "arithmetic",
			source: `
			.add  = .a + .b
			.sub  = .a - .b
			.mul  = .a * .b
			.div  = .a / .b
			.mod  = .a % .b
			.conc = .s + "_suffix"
		`,
			events: []eventCase{
				{
					in: `{"a":10,"b":3,"s":"hello"}`,
					fields: map[string]string{
						"add":  "13",
						"sub":  "7",
						"mul":  "30",
						"div":  "3",
						"mod":  "1",
						"conc": "hello_suffix",
					},
				},
			},
		},
		{
			name: "comparison",
			source: `
			.gt  = .a > .b
			.lt  = .a < .b
			.gte = .a >= .b
			.lte = .a <= .b
			.eq  = .a == .b
			.neq = .a != .b
			.seq = .s == "hello"
		`,
			events: []eventCase{
				{
					in: `{"a":10,"b":3,"s":"hello"}`,
					fields: map[string]string{
						"gt":  "true",
						"lt":  "false",
						"gte": "true",
						"lte": "false",
						"eq":  "false",
						"neq": "true",
						"seq": "true",
					},
				},
			},
		},
		{
			name: "logical",
			source: `
			.and  = .a && .b
			.or   = .b || .c
			.not  = !.c
		`,
			events: []eventCase{
				{
					in: `{"a":true,"b":false,"c":false}`,
					fields: map[string]string{
						"and": "false",
						"or":  "false",
						"not": "true",
					},
				},
			},
		},

		{
			name: "if_else",
			source: `
			if .status >= 500 {
				.severity = "critical"
			} else if .status >= 400 {
				.severity = "warning"
			} else {
				.severity = "ok"
			}
		`,
			events: []eventCase{
				{
					in:     `{"status":503}`,
					fields: map[string]string{"severity": "critical"},
				},
				{
					in:     `{"status":404}`,
					fields: map[string]string{"severity": "warning"},
				},
				{
					in:     `{"status":200}`,
					fields: map[string]string{"severity": "ok"},
				},
			},
		},

		{
			name: "abort",
			source: `
			if .drop == true {
				abort
			}
			.processed = true
		`,
			events: []eventCase{
				{
					in:     `{"drop":true}`,
					fields: map[string]string{"processed": ""},
				},
				{
					in:     `{"drop":false}`,
					fields: map[string]string{"processed": "true"},
				},
			},
		},
		{
			name: "path",
			source: `
			.user.role  = "admin"
			.tags[0]    = "first"
			idx         = 1
			.tags[idx]  = "second"
		`,
			events: []eventCase{
				{
					in: `{"user":{},"tags":["",""]}`,
					fields: map[string]string{
						"user.role": "admin",
						"tags.0":    "first",
						"tags.1":    "second",
					},
				},
			},
		},

		{
			name: "array",
			source: `
			arr       = [1, 2, 3]
			.first    = arr[0]
			.last     = arr[-1]
			arr[0]    = 99
			.modified = arr[0]
		`,
			events: []eventCase{
				{
					in: `{"x":1}`,
					fields: map[string]string{
						"first":    "1",
						"last":     "3",
						"modified": "99",
					},
				},
			},
		},

		{
			name: "object",
			source: `
			obj    = {"a": 1, "b": 2}
			.va    = obj["a"]
			.vb    = obj["b"]
		`,
			events: []eventCase{
				{
					in: `{"x":1}`,
					fields: map[string]string{
						"va": "1",
						"vb": "2",
					},
				},
			},
		},

		{
			name: "for_index",
			source: `
			for i in .items {
				if .items[i]["role"] == "admin" {
					.items[i]["privileged"] = true
				}
			}
		`,
			events: []eventCase{
				{
					in: `{"items":[{"role":"admin"},{"role":"user"}]}`,
					fields: map[string]string{
						"items.0.privileged": "true",
						"items.1.privileged": "",
					},
				},
			},
		},
		{
			name: "for_index_and_item",
			source: `
			for i, item in .items {
				if item["role"] == "admin" {
					.items[i]["privileged"] = true
				}
			}
		`,
			events: []eventCase{
				{
					in: `{"items":[{"role":"admin"},{"role":"user"}]}`,
					fields: map[string]string{
						"items.0.privileged": "true",
						"items.1.privileged": "",
					},
				},
			},
		},

		{
			name: "for_blank_index",
			source: `
			for _, item in .tags {
				.count = .count + 1
			}
		`,
			events: []eventCase{
				{
					in:     `{"tags":["a","b","c"],"count":0}`,
					fields: map[string]string{"count": "3"},
				},
			},
		},

		{
			name: "delete",
			source: `
			del .secret
			del .user.password
		`,
			events: []eventCase{
				{
					in: `{"secret":"s3cr3t","user":{"name":"user321","password":"123"}}`,
					fields: map[string]string{
						"secret":        "",
						"user.name":     "user321",
						"user.password": "",
					},
				},
				{
					in: `{"x":1}`,
					fields: map[string]string{
						"secret": "",
						"x":      "1",
					},
				},
			},
		},

		{
			name: "nested",
			source: `
			if .level == "error" || .level == "fatal" {
				.severity = "high"
			} else {
				.severity = "low"
			}

			for i, item in .errors {
				if item["code"] >= 500 {
					.errors[i]["critical"] = true
				}
			}

			del .internal
			.processed = true
		`,
			events: []eventCase{
				{
					in: `{"level":"error","errors":[{"code":503},{"code":404}],"internal":"secret"}`,
					fields: map[string]string{
						"severity":          "high",
						"errors.0.critical": "true",
						"errors.1.critical": "",
						"internal":          "",
						"processed":         "true",
					},
				},
				{
					in: `{"level":"info","errors":[{"code":200}],"internal":"secret"}`,
					fields: map[string]string{
						"severity":          "low",
						"errors.0.critical": "",
						"internal":          "",
						"processed":         "true",
					},
				},
			},
		},
		{
			name: "func_upcase",
			source: `
			.level = upcase(.level)
			name = upcase(.user.name)
			.user.name = name
		`,
			events: []eventCase{
				{
					in: `{"level":"info","user":{"name":"user321","password":"123"}}`,
					fields: map[string]string{
						"level":     "INFO",
						"user.name": "USER321",
					},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			config := test.NewConfig(&Config{Source: tc.source}, nil)
			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false),
				"name",
			)

			wg := &sync.WaitGroup{}
			outEvents := make([]string, 0, len(tc.events))

			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			wg.Add(len(tc.events))
			for _, ev := range tc.events {
				input.In(0, "test.log", test.NewOffset(0), []byte(ev.in))
			}
			wg.Wait()
			p.Stop()

			require.Equal(t, len(tc.events), len(outEvents), "wrong number of output events")

			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			for i, ev := range tc.events {
				err := root.DecodeString(outEvents[i])
				require.NoError(t, err, "event %d: failed to decode output JSON", i)

				for field, want := range ev.fields {
					node := root.Dig(cfg.ParseFieldSelector(field)...)
					got := ""
					if node != nil {
						got = node.AsString()
					}
					assert.Equal(t, want, got, "event %d: field %q", i, field)
				}
			}
		})
	}
}
