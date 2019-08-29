package filed

import (
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"regexp"
	"runtime"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Filed struct {
	config    *Config
	httpAddr  string
	plugins   *PluginRegistry
	pipelines []*pipeline.Pipeline
}

func New(config *Config, httpAddr string) *Filed {
	return &Filed{
		config:   config,
		httpAddr: httpAddr,
		plugins:  DefaultPluginRegistry,
	}
}

func NewWithPluginRegistry(config *Config, pluginRegistry *PluginRegistry) *Filed {
	return &Filed{
		config:  config,
		plugins: pluginRegistry,
	}
}

func (f *Filed) SetConfig(config *Config) {
	f.config = config
}

func (f *Filed) Start() {
	logger.Infof("starting filed")

	go f.startHttp()

	f.pipelines = f.pipelines[:0]
	for name, config := range f.config.pipelines {
		procs := runtime.GOMAXPROCS(0)
		tracksCount := procs * 4
		headsCount := procs * 4

		logger.Infof("starting pipeline %q using %d processor cores", name, procs)

		p := pipeline.New(name, tracksCount, headsCount)
		f.pipelines = append(f.pipelines, p)

		f.setupInput(config, name, p)
		f.setupActions(config, name, p)
		f.setupOutput(config, name, p)

		p.Start()
	}
}

func (f *Filed) setupInput(pipelineConfig *PipelineConfig, pipelineName string, p *pipeline.Pipeline) {
	inputJSON := pipelineConfig.raw.Get("input")
	if inputJSON.MustMap() == nil {
		logger.Fatalf("no input for pipeline %q", pipelineName)
	}
	t := inputJSON.Get("type").MustString()
	if t == "" {
		logger.Fatalf("no input type provided for pipeline %q", pipelineName)
	}

	logger.Infof("creating input with type %q", t)
	info := f.plugins.GetInputByType(t)
	configJson, err := inputJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for input %q in pipeline %q", t, pipelineName)
	}

	plugin, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		logger.Panicf("can't unmarshal config for input %q in pipeline %q", t, pipelineName)
	}

	p.SetInputPlugin(&pipeline.InputPluginDescription{Plugin: plugin.(pipeline.InputPlugin), Config: config, Type: t})
}

func (f *Filed) setupActions(pipelineConfig *PipelineConfig, pipelineName string, p *pipeline.Pipeline) {
	actions := pipelineConfig.raw.Get("actions")
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			logger.Fatalf("empty action #%d for pipeline %q", index, pipelineName)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			logger.Fatalf("action #%d doesn't provide type for pipeline %q", index, pipelineName)
		}

		logger.Infof("creating action with type %q", t)
		info := f.plugins.GetActionByType(t)
		configJson, err := actionJSON.Encode()
		if err != nil {
			logger.Panicf("can't create config json for action #%d in pipeline %q", index, t, pipelineName)
		}

		mm := actionJSON.Get("match_mode").MustString()
		if mm != "or" && mm != "and" && mm != "" {
			logger.Fatalf("unknown match mode %q for pipeline %q, must be or/and", mm, pipelineName)
		}

		matchMode := pipeline.ModeAnd
		if mm == "or" {
			matchMode = pipeline.ModeOr
		}

		conditions := make(pipeline.MatchConditions, 0, 0)
		condJSON := actionJSON.Get("match_fields")
		for field := range condJSON.MustMap() {
			value := strings.Trim(condJSON.Get(field).MustString(), " ")
			if value == "" {
				logger.Fatalf("no value for field matching condition %q in pipeline %q", field, pipelineName)
			}

			condition := pipeline.MatchCondition{
				Field: strings.Trim(field, " "),
			}
			if value[0] == '/' && value[len(value)-1] == '/' {
				r, err := regexp.Compile(value[1 : len(value)-2])
				if err != nil {
					logger.Fatalf("can't compile regexp %s: %s", value, err.Error())
				}
				condition.Regexp = r
			} else {
				condition.Value = value
			}
			conditions = append(conditions, condition)
		}

		for _, track := range p.Tracks {
			plugin, config := info.Factory()
			err = json.Unmarshal(configJson, config)
			if err != nil {
				logger.Panicf("can't unmarshal config for action #%d in pipeline %q", index, t, pipelineName)
			}

			track.AddActionPlugin(&pipeline.ActionPluginDescription{
				Plugin:          plugin.(pipeline.ActionPlugin),
				Config:          config,
				Type:            t,
				MatchConditions: conditions,
				MatchMode:       matchMode,
			})
		}
	}
}

func (f *Filed) setupOutput(pipelineConfig *PipelineConfig, pipelineName string, p *pipeline.Pipeline) {
	outputJSON := pipelineConfig.raw.Get("output")
	if outputJSON.MustMap() == nil {
		logger.Fatalf("no output for pipeline %q", pipelineName)
	}
	t := outputJSON.Get("type").MustString()
	if t == "" {
		logger.Fatalf("no output type provided for pipeline %q", pipelineName)
	}

	logger.Infof("creating output with type %q", t)
	info := f.plugins.GetOutputByType(t)
	configJson, err := outputJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for output %q in pipeline %q", t, pipelineName)
	}

	plugin, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		logger.Panicf("can't unmarshal config for output %q in pipeline %q", t, pipelineName)
	}

	p.SetOutputPlugin(&pipeline.OutputPluginDescription{Plugin: plugin.(pipeline.OutputPlugin), Config: config, Type: t})
}

func (f *Filed) Stop() {
	for _, p := range f.pipelines {
		p.Stop()
	}
}

func (f *Filed) startHttp() {
	if f.httpAddr == "off" {
		return
	}

	mux := http.DefaultServeMux

	mux.Handle("/live", liveReadyHandler)
	mux.Handle("/ready", liveReadyHandler)
	mux.Handle("/metrics", promhttp.Handler())

	err := http.ListenAndServe(f.httpAddr, mux)
	if err != nil {
		logger.Fatalf("can't start http with %q address: %s", f.httpAddr, err.Error())
	}
}

var liveReadyHandler = &liveReadyServer{}

type liveReadyServer struct {
}

func (p *liveReadyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.Infof("live/ready OK")
}
