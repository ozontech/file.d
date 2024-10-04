package test

import (
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type Opts []string

func (o Opts) Has(opt string) bool {
	return strings.Contains(strings.Join(o, "-"), opt)
}

type Case struct {
	Prepare func()
	Act     func(pipeline *pipeline.Pipeline)
	Assert  func(pipeline *pipeline.Pipeline)
	Out     func(event *pipeline.Event)
}

func RunCase(testCase *Case, inputInfo *pipeline.InputPluginInfo, eventCount int, pipelineOpts ...string) {
	testCase.Prepare()

	p := startCasePipeline(testCase.Act, testCase.Out, eventCount, inputInfo, pipelineOpts...)

	testCase.Assert(p)
}

func startCasePipeline(act func(pipeline *pipeline.Pipeline), out func(event *pipeline.Event), eventCount int, inputInfo *pipeline.InputPluginInfo, pipelineOpts ...string) *pipeline.Pipeline {
	x := atomic.NewInt32(int32(eventCount))

	pipelineOpts = append(pipelineOpts, "passive")
	p := NewPipeline(nil, pipelineOpts...)

	p.SetInput(inputInfo)

	anyPlugin, config := devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})

	outputPlugin.SetOutFn(func(event *pipeline.Event) {
		x.Dec()
		if out != nil {
			out(event)
		}
	})
	p.Start()

	act(p)

	t := time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		if x.Load() <= 0 {
			break
		}
		if time.Since(t) > time.Minute*2 {
			panic("too long act")
		}
	}
	p.Stop()

	return p
}

func WaitForEvents(x *atomic.Int32) {
	t := time.Now()
	for {
		time.Sleep(time.Millisecond * 10)
		if x.Load() <= 0 {
			break
		}
		if time.Since(t) > time.Second*10 {
			panic("too long wait")
		}
	}
}

func NewPipeline(actions []*pipeline.ActionPluginStaticInfo, pipelineOpts ...string) *pipeline.Pipeline {
	parallel := Opts(pipelineOpts).Has("parallel")
	perf := Opts(pipelineOpts).Has("perf")
	mock := Opts(pipelineOpts).Has("mock")
	passive := Opts(pipelineOpts).Has("passive")
	name := Opts(pipelineOpts).Has("name")

	eventTimeout := pipeline.DefaultEventTimeout
	if Opts(pipelineOpts).Has("short_event_timeout") {
		eventTimeout = 10 * time.Millisecond
	}

	capacity := 256
	if perf {
		parallel = true
		capacity = 20000
	}

	settings := &pipeline.Settings{
		Capacity:            capacity,
		MaintenanceInterval: time.Second * 5,
		EventTimeout:        eventTimeout,
		AntispamThreshold:   0,
		AvgEventSize:        2048,
		StreamField:         "stream",
		Decoder:             "json",
		MetricHoldDuration:  pipeline.DefaultMetricHoldDuration,
	}

	pName := "test_pipeline"
	if name {
		pName += strconv.Itoa(rand.Int())
	}

	p := pipeline.New(pName, settings, prometheus.NewRegistry())
	if !parallel {
		p.DisableParallelism()
	}

	if !perf {
		p.EnableEventLog()
	}

	if mock {
		anyPlugin, _ := fake.Factory()
		inputPlugin := anyPlugin.(*fake.Plugin)
		p.SetInput(&pipeline.InputPluginInfo{
			PluginStaticInfo: &pipeline.PluginStaticInfo{
				Type: "fake",
			},
			PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
				Plugin: inputPlugin,
			},
		})

		anyPlugin, _ = devnull.Factory()
		outputPlugin := anyPlugin.(*devnull.Plugin)
		p.SetOutput(&pipeline.OutputPluginInfo{
			PluginStaticInfo: &pipeline.PluginStaticInfo{
				Type: "devnull",
			},
			PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
				Plugin: outputPlugin,
			},
		})
	}

	for _, info := range actions {
		p.AddAction(info)
	}

	if !passive {
		p.Start()
	}

	return p
}

func NewPipelineMock(actions []*pipeline.ActionPluginStaticInfo, pipelineOpts ...string) (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	pipelineOpts = append(pipelineOpts, "mock")
	p := NewPipeline(actions, pipelineOpts...)

	return p, p.GetInput().(*fake.Plugin), p.GetOutput().(*devnull.Plugin)
}

func NewPluginStaticInfo(factory pipeline.PluginFactory, config pipeline.AnyConfig) *pipeline.PluginStaticInfo {
	return &pipeline.PluginStaticInfo{
		Type:    "test_plugin",
		Factory: factory,
		Config:  config,
	}
}

func NewActionPluginStaticInfo(factory pipeline.PluginFactory, config pipeline.AnyConfig, mode pipeline.MatchMode, conds pipeline.MatchConditions, matchInvert bool) []*pipeline.ActionPluginStaticInfo {
	return []*pipeline.ActionPluginStaticInfo{
		{
			PluginStaticInfo: NewPluginStaticInfo(factory, config),
			MatchConditions:  conds,
			MatchMode:        mode,
			MatchInvert:      matchInvert,
		},
	}
}

func NewEmptyOutputPluginParams() *pipeline.OutputPluginParams {
	return &pipeline.OutputPluginParams{
		PluginDefaultParams: newDefaultParams(),
		Logger:              newLogger().Named("output"),
	}
}

func NewEmptyActionPluginParams() *pipeline.ActionPluginParams {
	return &pipeline.ActionPluginParams{
		PluginDefaultParams: newDefaultParams(),
		Logger:              newLogger().Named("action"),
	}
}

func newLogger() *zap.SugaredLogger {
	lgCfg := zap.NewDevelopmentConfig()
	lgCfg.Level.SetLevel(zapcore.WarnLevel)
	lg, err := lgCfg.Build()
	if err != nil {
		panic(err)
	}
	return lg.Sugar()
}

func newDefaultParams() pipeline.PluginDefaultParams {
	return pipeline.PluginDefaultParams{
		PipelineName:     "test_pipeline",
		PipelineSettings: &pipeline.Settings{},
		MetricCtl:        metric.NewCtl("test", prometheus.NewRegistry()),
	}
}

func NewConfig(config any, params map[string]int) any {
	err := cfg.SetDefaultValues(config)
	if err != nil {
		logger.Panicf("cannot set defaults for config: %s", err.Error())
	}

	err = cfg.Parse(config, params)
	if err != nil {
		logger.Panicf("wrong config: %s", err.Error())
	}

	return config
}

type Offset int64

func (o Offset) Current() int64 {
	return int64(o)
}

func (o Offset) ByStream(_ string) int64 {
	panic("unimplemented")
}
