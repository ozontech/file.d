package set_time

import (
	"fmt"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
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
				Field:   "time",
				Format_: "timestamp",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":%d}`, now.Unix()),
		},
		{
			Name: "unix",
			Config: &Config{
				Field:   "time",
				Format_: "timestampnano",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":%d}`, now.UnixNano()),
		},
		{
			Name: "custom format",
			Config: &Config{
				Field:   "my-time",
				Format_: "2006-01-02",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"my-time":"%s"}`, now.Format("2006-01-02")),
		},
		{
			Name: "rfc3339",
			Config: &Config{
				Field:   "myTime",
				Format_: "rfc3339",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"myTime":"%s"}`, now.Format(time.RFC3339)),
		},
		{
			Name: "override false",
			Config: &Config{
				Field:    "time",
				Format_:  "test",
				Override: false,
			},
			Root: `{"time":123}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   `{"time":123}`,
		},
		{
			Name: "override true",
			Config: &Config{
				Field:    "time",
				Format_:  time.RFC3339,
				Override: true,
			},
			Root: `{"time":123}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":"%s"}`, now.Format(time.RFC3339)),
		},
		{
			Name: "dots field",
			Config: &Config{
				Format_:  "timestampmilli",
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

			plugin := &Plugin{
				config: tc.Config,
			}
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
	config := &Config{Format: "timestamp", Field: "timestamp"}

	err := cfg.Parse(config, nil)
	require.NoError(t, err)

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	outEvents := make([]*pipeline.Event, 0)
	counter := atomic.Int32{}
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		counter.Dec()
	})

	counter.Add(1)
	input.In(0, "test.log", 0, []byte(`{"message":123}`))

	for counter.Load() != 0 {
		time.Sleep(time.Millisecond * 10)
	}
	p.Stop()

	require.Equal(t, 1, len(outEvents), "wrong out events count")
	timestamp := outEvents[0].Root.Dig("timestamp")
	require.NotNil(t, timestamp)
	require.NotEqual(t, "", timestamp.EncodeToString(), "wrong out event")
}
