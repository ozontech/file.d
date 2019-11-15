package filed

import (
	"encoding/json"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"

	"github.com/bitly/go-simplejson"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Filed struct {
	config    *Config
	httpAddr  string
	registry  *prometheus.Registry
	plugins   *PluginRegistry
	Pipelines []*pipeline.Pipeline
	server    *http.Server
}

func New(config *Config, httpAddr string) *Filed {
	return &Filed{
		config:    config,
		httpAddr:  httpAddr,
		plugins:   DefaultPluginRegistry,
		Pipelines: make([]*pipeline.Pipeline, 0, 0),
	}
}

func (f *Filed) SetConfig(config *Config) {
	f.config = config
}

func (f *Filed) Start() {
	logger.Infof("starting filed")

	f.createRegistry()
	f.startHTTP()
	f.startPipelines()
}

func (f *Filed) createRegistry() {
	f.registry = prometheus.NewRegistry()
	f.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f.registry.MustRegister(prometheus.NewGoCollector())

	prometheus.DefaultGatherer = f.registry
	prometheus.DefaultRegisterer = f.registry
}

func (f *Filed) startPipelines() {
	f.Pipelines = f.Pipelines[:0]
	for name, config := range f.config.Pipelines {
		f.addPipeline(name, config)
	}
	for _, p := range f.Pipelines {
		p.Start()
	}
}

func (f *Filed) addPipeline(name string, config *PipelineConfig) {
	mux := http.DefaultServeMux
	settings := extractPipelineParams(config.Raw.Get("settings"))

	p := pipeline.New(name, settings, f.registry, mux)
	f.setupInput(p, config)
	f.setupActions(p, config)
	f.setupOutput(p, config)

	f.Pipelines = append(f.Pipelines, p)
}

func (f *Filed) setupInput(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) {
	inputJSON := pipelineConfig.Raw.Get("input")
	if inputJSON.MustMap() == nil {
		logger.Fatalf("no input for pipeline %q", p.Name)
	}
	t := inputJSON.Get("type").MustString()
	if t == "" {
		logger.Fatalf("no input type provided for pipeline %q", p.Name)
	}

	logger.Infof("creating input with type %q", t)
	info := f.plugins.GetInputByType(t)
	configJson, err := inputJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for input %q in pipeline %q", t, p.Name)
	}

	plugin, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		logger.Panicf("can't unmarshal config for input %q in pipeline %q", t, p.Name)
	}

	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: plugin.(pipeline.InputPlugin), PluginDesc: pipeline.PluginDesc{Config: config, T: t}})
}

func (f *Filed) setupActions(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) {
	actions := pipelineConfig.Raw.Get("actions")
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			logger.Fatalf("empty action #%d for pipeline %q", index, p.Name)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			logger.Fatalf("action #%d doesn't provide type for pipeline %q", index, p.Name)
		}
		f.setupAction(p, index, t, actionJSON)
	}
}

func (f *Filed) setupAction(p *pipeline.Pipeline, index int, t string, actionJSON *simplejson.Json) {
	logger.Infof("creating action with type %q for pipeline %q", t, p.Name)
	info := f.plugins.GetActionByType(t)

	matchMode, err := extractMatchMode(actionJSON)
	if err != nil {
		logger.Fatalf("can't extract match mode for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		logger.Fatalf("can't extract conditions for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	metricName, metricLabels := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)

	p.AddAction(info, configJSON, matchMode, conditions, metricName, metricLabels)
}

func (f *Filed) setupOutput(p *pipeline.Pipeline, pipelineConfig *PipelineConfig) {
	outputJSON := pipelineConfig.Raw.Get("output")
	if outputJSON.MustMap() == nil {
		logger.Fatalf("no output for pipeline %q", p.Name)
	}
	t := outputJSON.Get("type").MustString()
	if t == "" {
		logger.Fatalf("no output type provided for pipeline %q", p.Name)
	}

	logger.Infof("creating output with type %q", t)
	info := f.plugins.GetOutputByType(t)
	configJson, err := outputJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for output %q in pipeline %q", t, p.Name)
	}

	plugin, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		logger.Panicf("can't unmarshal config for output %q in pipeline %q", t, p.Name)
	}

	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: plugin.(pipeline.OutputPlugin), PluginDesc: pipeline.PluginDesc{Config: config, T: t}})
}

func (f *Filed) Stop() {
	logger.Infof("stopping filed pipelines=%d", len(f.Pipelines))
	_ = f.server.Shutdown(nil)
	for _, p := range f.Pipelines {
		p.Stop()
	}
}

func (f *Filed) startHTTP() {
	if f.httpAddr == "off" {
		return
	}

	mux := http.DefaultServeMux

	mux.HandleFunc("/live", f.serveLiveReady)
	mux.HandleFunc("/ready", f.serveLiveReady)
	mux.HandleFunc("/freeosmem", f.serveFreeOsMem)
	mux.Handle("/metrics", promhttp.Handler())

	f.server = &http.Server{Addr: f.httpAddr, Handler: mux}
	go f.listenHTTP()
}

func (f *Filed) listenHTTP() {
	err := f.server.ListenAndServe()
	if err != nil {
		logger.Fatalf("http listening error address=%q: %s", f.httpAddr, err.Error())
	}
}

func (f *Filed) serveFreeOsMem(w http.ResponseWriter, r *http.Request) {
	debug.FreeOSMemory()
	logger.Infof("Free OS memory OK")
}

func (f *Filed) serveLiveReady(w http.ResponseWriter, r *http.Request) {
	logger.Infof("live/ready OK")
}

func (f *Filed) servePipelines(w http.ResponseWriter, r *http.Request) {
	logger.Infof("pipelines OK")
}
