package set_time

import (
	"fmt"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestPlugin_Do(t *testing.T) {
	now, err := time.Parse(time.RFC3339, "2006-01-02T15:04:05+07:00")
	require.NoError(t, err)

	type TestCase struct {
		Name   string
		Config *Config
		Root   string

		ExpResult pipeline.ActionResult
		ExpRoot   string
	}

	tcs := []TestCase{
		{
			Name: "unix",
			Config: &Config{
				Format: pipeline.UnixTime,
				Field:  "time",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":%d}`, now.Unix()),
		},
		{
			Name: "unix nano",
			Config: &Config{
				Format: "timestampnano",
				Field:  "time",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":%d}`, now.UnixNano()),
		},
		{
			Name: "custom format",
			Config: &Config{
				Format: "2006-01-02",
				Field:  "my-time",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"my-time":"%s"}`, now.Format("2006-01-02")),
		},
		{
			Name: "rfc3339",
			Config: &Config{
				Format: "rfc3339",
				Field:  "myTime",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"myTime":"%s"}`, now.Format(time.RFC3339)),
		},
		{
			Name: "override false",
			Config: &Config{
				Format:   "test",
				Field:    "time",
				Override: false,
			},
			Root: `{"time":123}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   `{"time":123}`,
		},
		{
			Name: "override true",
			Config: &Config{
				Format:   time.RFC3339,
				Field:    "time",
				Override: true,
			},
			Root: `{"time":123}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":"%s"}`, now.Format(time.RFC3339)),
		},
		{
			Name: "dots field",
			Config: &Config{
				Format:   "timestampmilli",
				Field:    "a.b.c",
				Override: true,
			},
			Root: `{"a":{"b":{"c":123}}}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"a":{"b":{"c":123}},"a.b.c":%d}`, now.UnixMilli()),
		},
	}

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)

	for _, tc := range tcs {
		t.Run(tc.Name, func(t *testing.T) {
			require.NoError(t, root.DecodeString(tc.Root))
			cfg.Parse(tc.Config, nil)

			plugin := &Plugin{}
			event := &pipeline.Event{
				Root: root,
			}

			plugin.Start(tc.Config, nil)
			result := plugin.do(event, now)

			require.Equal(t, tc.ExpResult, result)
			require.Equal(t, tc.ExpRoot, event.Root.EncodeToString())
		})
	}
}

func TestE2E_Plugin(t *testing.T) {
	config := test.NewConfig(&Config{Format: pipeline.UnixTime, Field: "timestamp"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	counter := atomic.Int32{}
	output.SetOutFn(func(e *pipeline.Event) {
		require.NotEqual(t, "", e.Root.Dig("timestamp").AsString(), "wrong out event")
		counter.Dec()
	})

	counter.Add(1)
	input.In(0, "test.log", test.Offset(0), []byte(`{"message":123}`))

	for counter.Load() != 0 {
		time.Sleep(time.Millisecond * 10)
	}
	p.Stop()
}
