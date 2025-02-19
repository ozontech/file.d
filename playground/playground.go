package playground

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	_ "github.com/ozontech/file.d/plugin/action/add_file_name"
	_ "github.com/ozontech/file.d/plugin/action/add_host"
	_ "github.com/ozontech/file.d/plugin/action/convert_date"
	_ "github.com/ozontech/file.d/plugin/action/convert_log_level"
	_ "github.com/ozontech/file.d/plugin/action/convert_utf8_bytes"
	_ "github.com/ozontech/file.d/plugin/action/debug"
	_ "github.com/ozontech/file.d/plugin/action/decode"
	_ "github.com/ozontech/file.d/plugin/action/discard"
	_ "github.com/ozontech/file.d/plugin/action/flatten"
	_ "github.com/ozontech/file.d/plugin/action/join"
	_ "github.com/ozontech/file.d/plugin/action/join_template"
	_ "github.com/ozontech/file.d/plugin/action/json_decode"
	_ "github.com/ozontech/file.d/plugin/action/json_encode"
	_ "github.com/ozontech/file.d/plugin/action/json_extract"
	_ "github.com/ozontech/file.d/plugin/action/keep_fields"
	_ "github.com/ozontech/file.d/plugin/action/mask"
	_ "github.com/ozontech/file.d/plugin/action/modify"
	_ "github.com/ozontech/file.d/plugin/action/move"
	_ "github.com/ozontech/file.d/plugin/action/parse_es"
	_ "github.com/ozontech/file.d/plugin/action/parse_re2"
	_ "github.com/ozontech/file.d/plugin/action/remove_fields"
	_ "github.com/ozontech/file.d/plugin/action/rename"
	_ "github.com/ozontech/file.d/plugin/action/set_time"
	_ "github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type playground struct {
	logger         *zap.Logger
	nextPipelineID *atomic.Int64
}

func newPlayground(logger *zap.Logger) *playground {
	return &playground{
		logger:         logger,
		nextPipelineID: new(atomic.Int64),
	}
}

func (h *playground) Play(ctx context.Context, req PlayRequest) (PlayResponse, error) {
	if req.Debug {
		var err error
		req.Actions, err = debugActions(req.Actions)
		if err != nil {
			return PlayResponse{}, err
		}
	}

	// Setup pipeline buffer.
	stdoutBuf := new(bytes.Buffer)
	stdoutBuf.Grow(1 << 10)
	var fatalErr error
	stdout := preparePipelineLogger(stdoutBuf, zapHookFunc(func(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
		fatalPayload := formatZapFields(fields)
		fatalErr = fmt.Errorf("fatal: %s: %s", entry.Message, fatalPayload.String())
	}))

	pipelineName := fmt.Sprintf("playground_%d", h.nextPipelineID.Inc())
	settings := &pipeline.Settings{
		Decoder:             "json",
		DecoderParams:       nil,
		Capacity:            pipelineCapacity,
		MaintenanceInterval: time.Millisecond * 100,
		EventTimeout:        time.Millisecond * 100,
		IsStrict:            false,
		MetricHoldDuration:  time.Minute,
		Pool:                pipeline.PoolTypeLowMem,
	}
	metricsRegistry := prometheus.NewRegistry()
	p := pipeline.New(pipelineName, settings, metricsRegistry, stdout)
	// Check logger.Fatal() calls.
	if fatalErr != nil {
		return PlayResponse{}, fatalErr
	}

	// Callback to collect output events.
	events := make(chan json.RawMessage, len(req.Events))
	outputCb := func(event *pipeline.Event) {
		events <- event.Root.EncodeToByte()
	}

	if err := setupPipeline(p, req, outputCb); err != nil {
		return PlayResponse{}, err
	}
	if fatalErr != nil {
		return PlayResponse{}, fatalErr
	}

	p.Start()
	if fatalErr != nil {
		return PlayResponse{}, fatalErr
	}

	// Push events to the pipeline.
	for i, event := range req.Events {
		p.In(pipeline.SourceID(i+1), "fake", pipeline.NewOffsets(int64(i+1), nil), event, true, nil)
	}

	// Collect result.
	var result []json.RawMessage
loop:
	for {
		select {
		case <-ctx.Done():
			h.logger.Warn("request timed out") // e.g. some events were discarded
			break loop
		case event := <-events:
			result = append(result, event)
			if len(result) >= len(req.Events) {
				break loop
			}
		}
	}
	p.Stop()
	if fatalErr != nil {
		return PlayResponse{}, fatalErr
	}

	// Collect metrics.
	metricsInfo, err := metricsRegistry.Gather()
	if err != nil {
		h.logger.Error("can't gather metrics", zap.Error(err))
	}

	_ = stdout.Sync()

	return PlayResponse{
		Result:  result,
		Stdout:  stdoutBuf.String(),
		Metrics: formatMetricFamily(metricsInfo),
	}, nil
}

func formatZapFields(fields []zapcore.Field) strings.Builder {
	enc := zapcore.NewMapObjectEncoder()
	for _, field := range fields {
		field.AddTo(enc)
	}
	fatalPayload := strings.Builder{}
	for key, value := range enc.Fields {
		if fatalPayload.Len() > 0 {
			fatalPayload.WriteString("; ")
		}
		fatalPayload.WriteString(fmt.Sprintf("%q=%q", key, value))
	}
	return fatalPayload
}

// debugActions wraps all req.Action with 'debug' action.
func debugActions(actions []json.RawMessage) ([]json.RawMessage, error) {
	newActions := make([]json.RawMessage, 0, len(actions)*2)
	for _, action := range actions {
		// Parse name of the next action.
		var actionWithType = struct {
			Type string `json:"type"`
		}{}
		if err := json.Unmarshal(action, &actionWithType); err != nil {
			return nil, err
		}
		if actionWithType.Type == "" {
			return nil, fmt.Errorf("action type is empty")
		}
		actionType := escapeJSON(actionWithType.Type)

		debugActionBefore := json.RawMessage(fmt.Sprintf(`{"type": "debug", "message": "before %s"}`, actionType))
		debugActionAfter := json.RawMessage(fmt.Sprintf(`{"type": "debug", "message": "after %s"}`, actionType))
		// Wrap current action plugin with debug actions.
		newActions = append(newActions, debugActionBefore, action, debugActionAfter)
	}
	return newActions, nil
}

var pluginRegistry = fd.DefaultPluginRegistry

func setupPipeline(p *pipeline.Pipeline, req PlayRequest, cb func(event *pipeline.Event)) error {
	if req.Actions == nil {
		req.Actions = []json.RawMessage{[]byte(`[]`)}
	}

	actionsArray, _ := json.Marshal(req.Actions)
	actionsRaw, err := simplejson.NewJson(actionsArray)
	if err != nil {
		return fmt.Errorf("read actions: %w", err)
	}
	values := map[string]int{
		"capacity":   pipelineCapacity,
		"gomaxprocs": runtime.GOMAXPROCS(0),
	}
	if err := fd.SetupActions(p, pluginRegistry, actionsRaw, values); err != nil {
		return err
	}

	// Setup input fake plugin.
	sharedInputStaticInfo, err := pluginRegistry.Get(pipeline.PluginKindInput, "fake")
	if err != nil {
		return err
	}
	inputStaticInfo := *sharedInputStaticInfo

	inputPlugin, config := inputStaticInfo.Factory()
	inputStaticInfo.Config = config

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &inputStaticInfo,
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: inputPlugin,
		},
	})

	// Setup output fake plugin.
	sharedOutputStaticInfo, err := pluginRegistry.Get(pipeline.PluginKindOutput, "devnull")
	if err != nil {
		return err
	}
	outputStaticInfo := *sharedOutputStaticInfo

	outputPlugin, config := outputStaticInfo.Factory()
	outputStaticInfo.Config = config

	outputPlugin.(*devnull.Plugin).SetOutFn(cb)

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &outputStaticInfo,
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})
	return nil
}

func preparePipelineLogger(buf *bytes.Buffer, onFatal zapcore.CheckWriteHook) *zap.Logger {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout("05.000000")

	stdout := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(encoderConfig),
			zapcore.AddSync(buf),
			zapcore.DebugLevel,
		),
		zap.WithFatalHook(onFatal),
		zap.WithCaller(false),
		zap.WithClock(newZeroClock(time.Now())),
	)
	return stdout
}

func formatMetricFamily(families []*dto.MetricFamily) string {
	b := new(bytes.Buffer)
	for _, f := range families {
		_ = expfmt.NewEncoder(b, expfmt.FmtOpenMetrics).Encode(f)
		b.WriteString("\n")
	}
	return b.String()
}

func escapeJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	// Trim the beginning and trailing " character
	return string(b[1 : len(b)-1])
}

// zeroClock reports time since "start".
// Used to print relative time rather than absolute.
type zeroClock struct {
	start time.Time
}

var _ zapcore.Clock = zeroClock{}

func newZeroClock(start time.Time) *zeroClock {
	return &zeroClock{start: start}
}

func (z zeroClock) Now() time.Time {
	diff := time.Since(z.start)
	return time.Time{}.Add(diff)
}

func (z zeroClock) NewTicker(_ time.Duration) *time.Ticker {
	return new(time.Ticker)
}

type zapHookFunc func(*zapcore.CheckedEntry, []zapcore.Field)

func (f zapHookFunc) OnWrite(entry *zapcore.CheckedEntry, fields []zapcore.Field) {
	f(entry, fields)
}
