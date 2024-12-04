package convert_log_level

import (
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestDo(t *testing.T) {
	type TestCase struct {
		Name   string
		Config Config
		In     []string
		Out    []string
	}

	tcs := []TestCase{
		{
			Name: "it works with strings",
			Config: Config{
				Field: "info.level",
				Style: "string",
			},
			In:  []string{`{"info":{"level":"1"}}`},
			Out: []string{`{"info":{"level":"alert"}}`},
		},
		{
			Name: "it works with numbers",
			Config: Config{
				Field: "info.level",
				Style: "number",
			},
			In:  []string{`{"info":{"level":"alert"}}`},
			Out: []string{`{"info":{"level":1}}`},
		},
		{
			Name: "pass if object",
			Config: Config{
				Field: "info.level",
				Style: "number",
			},
			In:  []string{`{"info":{"level":{"a":"b"}}}`},
			Out: []string{`{"info":{"level":{"a":"b"}}}`},
		},
		{
			Name: "remove if object without default",
			Config: Config{
				Field:        "info.level",
				Style:        "number",
				DefaultLevel: "",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":{"a":"b"}}}`},
			Out: []string{`{"info":{}}`},
		},
		{
			Name: "override if object",
			Config: Config{
				Field:        "info.level",
				Style:        "number",
				DefaultLevel: "alert",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":{"a":"b"}}}`},
			Out: []string{`{"info":{"level":1}}`},
		},
		{
			Name: "pass if number",
			Config: Config{
				Field:        "info.level",
				Style:        "string",
				DefaultLevel: "5",
			},
			In:  []string{`{"info":{"level": 5}}`},
			Out: []string{`{"info":{"level":"notice"}}`},
		},
		{
			Name: "pass if number with RemoveOnFail",
			Config: Config{
				Field:        "info.level",
				Style:        "string",
				DefaultLevel: "5",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level": 5}}`},
			Out: []string{`{"info":{"level":"notice"}}`},
		},
		{
			Name: "remove on fail parse number",
			Config: Config{
				Field:        "info.level",
				Style:        "number",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":"UNKNOWN"}}`},
			Out: []string{`{"info":{}}`},
		},
		{
			Name: "remove on fail parse string",
			Config: Config{
				Field:        "info.level",
				Style:        "string",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":"UNKNOWN"}}`},
			Out: []string{`{"info":{}}`},
		},
		{
			Name: "remove on fail parse with default level",
			Config: Config{
				Field:        "info.level",
				Style:        "number",
				DefaultLevel: "1",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":"UNKNOWN"}}`},
			Out: []string{`{"info":{}}`},
		},
		{
			Name: "default level with empty input",
			Config: Config{
				Field:        "info.level",
				Style:        "number",
				DefaultLevel: "4",
				RemoveOnFail: true,
			},
			In:  []string{`{"info":{"level":""}}`},
			Out: []string{`{"info":{"level":4}}`},
		},
		{
			Name: "empty field",
			Config: Config{
				Field: "info.level",
				Style: "number",
			},
			In:  []string{`{}`},
			Out: []string{`{}`},
		},
		{
			Name: "must add nested field",
			Config: Config{
				Field:        "info.level",
				Style:        "string",
				DefaultLevel: "alert",
			},
			In:  []string{`{}`},
			Out: []string{`{"info":{"level":"alert"}}`},
		},
		{
			Name: "must add nested field to the existing object",
			Config: Config{
				Field:        "info.level.value",
				Style:        "string",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":{"level":{}}}`},
			Out: []string{`{"info":{"level":{"value":"alert"}}}`},
		},
		{
			Name: "override if field is array",
			Config: Config{
				Field:        "info.level.value",
				Style:        "string",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":[]}`},
			Out: []string{`{"info":{"level":{"value":"alert"}}}`},
		},
		{
			Name: "override if default is not set",
			Config: Config{
				Field: "info.level",
				Style: "number",
			},
			In:  []string{`{"info":{"level":[5]}}`},
			Out: []string{`{"info":{"level":[5]}}`},
		},
		{
			Name: "override if field is nested array",
			Config: Config{
				Field:        "info.level.a.b.c.value",
				Style:        "number",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":{"level":{"a":{"b":[]}}}}`},
			Out: []string{`{"info":{"level":{"a":{"b":{"c":{"value":1}}}}}}`},
		},
		{
			Name: "override if field is nested number",
			Config: Config{
				Field:        "info.level.a.b.c.value",
				Style:        "string",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":{"level":{"a":{"b":4}}}}`},
			Out: []string{`{"info":{"level":{"a":{"b":{"c":{"value":"alert"}}}}}}`},
		},
		{
			Name: "create nested field with default level",
			Config: Config{
				Field:        "info.level.a.b.c.value",
				Style:        "string",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":{"level":{"a":{}}}}`},
			Out: []string{`{"info":{"level":{"a":{"b":{"c":{"value":"alert"}}}}}}`},
		},
		{
			Name: "create nested field with default level and number style",
			Config: Config{
				Field:        "info.level.a.b.c.value",
				Style:        "number",
				DefaultLevel: "alert",
			},
			In:  []string{`{"info":{"level":{"a":{}}}}`},
			Out: []string{`{"info":{"level":{"a":{"b":{"c":{"value":1}}}}}}`},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			config := test.NewConfig(&tc.Config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

			outCounter := atomic.NewInt32(int32(len(tc.In)))

			var outEvents []string
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				outCounter.Dec()
			})

			for _, log := range tc.In {
				input.In(0, "test.log", test.Offset(0), []byte(log))
			}

			now := time.Now()
			for {
				if outCounter.Load() == 0 || time.Since(now) > time.Second*20 {
					break
				}
				time.Sleep(time.Millisecond * 10)
			}
			p.Stop()

			require.Equal(t, tc.Out, outEvents)
		})
	}
}
