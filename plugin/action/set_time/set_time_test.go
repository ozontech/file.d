package set_time

import (
	"fmt"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
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
				Field_:  []string{"time"},
				Format_: "timestamp",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"time":%d}`, now.Unix()),
		},
		{
			Name: "custom format",
			Config: &Config{
				Field_:  []string{"my-time"},
				Format_: "2006-01-02",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"my-time":"%s"}`, now.Format("2006-01-02")),
		},
		{
			Name: "known format",
			Config: &Config{
				Field_:  []string{"myTime"},
				Format_: "rfc3339",
			},
			Root: `{}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   fmt.Sprintf(`{"myTime":"%s"}`, now.Format(time.RFC3339)),
		},
		{
			Name: "override",
			Config: &Config{
				Override: false,
			},
			Root: `{"time":123}`,

			ExpResult: pipeline.ActionPass,
			ExpRoot:   `{"time":123}`,
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
