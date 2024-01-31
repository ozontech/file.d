package debug

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestLogger(t *testing.T) {
	r := require.New(t)
	core, zapObserver := observer.New(zapcore.DebugLevel)
	lg := zap.New(core)

	p := Plugin{}
	c := Config{
		Interval_:  time.Second,
		First:      5,
		Thereafter: 0,
		Message:    "input event sample",
	}

	const pipelineName = "logd"
	p.Start(&c, &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{
			PipelineName: pipelineName,
		},
		Logger: lg.Sugar(),
	})

	loggerByPipelineMu.Lock()
	_, ok := loggerByPipeline[pipelineName]
	r.True(ok)
	loggerByPipelineMu.Unlock()

	const log = `{"hello":"world"}`
	root, err := insaneJSON.DecodeString(log)
	r.NoError(err)
	defer insaneJSON.Release(root)

	p.Do(&pipeline.Event{
		Root: root,
	})

	r.Equal(1, zapObserver.Len())
	logged := zapObserver.All()[0]

	r.Equal(c.Message, logged.Message)

	eventIdx := slices.IndexFunc(logged.Context, func(field zapcore.Field) bool { return field.Key == eventField })
	r.NotEqual(-1, eventIdx)
	r.Equal(log, string(logged.Context[eventIdx].Interface.(json.RawMessage)))
}
