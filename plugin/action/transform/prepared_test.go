package transform

import (
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestPluginPreparedCalls(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		input   string
		want    string
		message string
		err     string
	}{
		{
			name: "success",
			source: `
				.saved = trim_right(.value, "!")
				.out = before(.saved, "-")
			`,
			input: `{"value":"a-b!!"}`,
			want:  `{"value":"a-b!!","saved":"a-b","out":"a"}`,
		},
		{
			name: "simple string calls",
			source: `
				.saved = trim(.value, " ")
				.saved = trim_left(.saved, "!")
				.saved = trim_to_right(.saved, "|")
				.saved = trim_to_left(.saved, "|")
				.saved = trim_to(.saved, "|")
				.out = upcase(after(between(.saved, "|", "|"), "prefix-"))
			`,
			input: `{"value":" !!|prefix-data|!! "}`,
			want:  `{"value":" !!|prefix-data|!! ","saved":"|prefix-data|","out":"DATA"}`,
		},
		{
			name: "named and collection calls",
			source: `
				.saved = parse_regex(.value, r'(?P<prefix>\p{L}+)', numeric_groups: true)
				.parts = parse_regex_all(.value, r'(\d+)', limit: 2, group: 1)
				.out = join([slice(.value, 0, end: 2), to_string(lookup(.parts[0], {"12": 42}, default: null))], ":")
			`,
			input: `{"value":"界a12 b34 c56"}`,
			want:  `{"value":"界a12 b34 c56","saved":{"prefix":"界a","0":"界a","1":"界a"},"parts":["12","34"],"out":"界a:42"}`,
		},
		{
			name: "regex operation error",
			source: `
				.saved = join(["a", "b"], ",")
				.out = parse_regex_all(.value, r'(\d+)', group: 2)
				.after = "unreachable"
			`,
			input:   `{"value":"12"}`,
			want:    `{"value":"12","saved":"a,b"}`,
			message: "transform runtime error",
			err:     "parse_regex_all: group 2 is out of range: pattern has 1 capture groups",
		},
		{
			name: "runtime error",
			source: `
				.saved = trim_right(.value, "!")
				.out = before(null, "-")
				.after = "unreachable"
			`,
			input:   `{"value":"a-b!!"}`,
			want:    `{"value":"a-b!!","saved":"a-b"}`,
			message: "transform runtime error",
			err:     `function "before": argument "value": expected string, got null`,
		},
		{
			name: "abort",
			source: `
				.saved = trim_right(.value, "!")
				abort
				.out = before(null, "-")
			`,
			input:   `{"value":"a-b!!"}`,
			want:    `{"value":"a-b!!","saved":"a-b"}`,
			message: "transform program aborted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			logCore, logs := observer.New(zap.DebugLevel)
			p := &Plugin{}
			p.Start(&Config{Source: tt.source}, &pipeline.ActionPluginParams{
				PluginDefaultParams: pipeline.PluginDefaultParams{PipelineName: t.Name()},
				Logger:              zap.New(logCore).Sugar(),
			})
			t.Cleanup(p.Stop)
			assign, ok := p.program.expressions[0].(*core.AssignExpr)
			require.True(t, ok)
			call, ok := assign.Value.(*core.CallExpr)
			require.True(t, ok)
			require.NotNil(t, call.Plan, "Start must publish a prepared program")
			logs.TakeAll()
			root, err := insaneJSON.DecodeString(tt.input)
			require.NoError(t, err)
			defer insaneJSON.Release(root)

			result := p.Do(&pipeline.Event{Root: root})

			require.Equal(t, pipeline.ActionPass, result)
			require.JSONEq(t, tt.want, root.EncodeToString())
			if tt.message == "" {
				require.Zero(t, logs.Len())
			} else {
				require.Equal(t, 1, logs.Len())
				require.Equal(t, tt.message, logs.All()[0].Message)
				if tt.err != "" {
					require.Contains(t, logs.All()[0].ContextMap()["error"], tt.err)
				}
			}
		})
	}
}
