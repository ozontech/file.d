package fd

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"runtime/debug"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type FileD struct {
	config    *cfg.Config
	httpAddr  string
	registry  *prometheus.Registry
	plugins   *PluginRegistry
	Pipelines []*pipeline.Pipeline
	server    *http.Server
}

func New(config *cfg.Config, httpAddr string) *FileD {
	return &FileD{
		config:    config,
		httpAddr:  httpAddr,
		plugins:   DefaultPluginRegistry,
		Pipelines: make([]*pipeline.Pipeline, 0),
	}
}

func (f *FileD) SetConfig(config *cfg.Config) {
	f.config = config
}

func (f *FileD) Start() {
	logger.Infof("starting file.d")

	f.createRegistry()
	f.startHTTP()
	f.startPipelines()
}

func (f *FileD) createRegistry() {
	f.registry = prometheus.NewRegistry()
	f.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f.registry.MustRegister(prometheus.NewGoCollector())

	prometheus.DefaultGatherer = f.registry
	prometheus.DefaultRegisterer = f.registry
}

func (f *FileD) startPipelines() {
	f.Pipelines = f.Pipelines[:0]
	for name, config := range f.config.Pipelines {
		f.addPipeline(name, config)
	}
	for _, p := range f.Pipelines {
		p.Start()
	}
}

func (f *FileD) addPipeline(name string, config *cfg.PipelineConfig) {
	mux := http.DefaultServeMux
	settings := extractPipelineParams(config.Raw.Get("settings"))

	values := map[string]int{
		"capacity":   settings.Capacity,
		"gomaxprocs": runtime.GOMAXPROCS(0),
	}

	logger.Infof("creating pipeline %q: capacity=%d, stream field=%s, decoder=%s", name, settings.Capacity, settings.StreamField, settings.Decoder)

	p := pipeline.New(name, settings, f.registry)
	err := f.setupInput(p, config, values)
	if err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

	f.setupActions(p, config, values)

	err = f.setupOutput(p, config, values)
	if err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

	p.SetupHTTPHandlers(mux)
	f.Pipelines = append(f.Pipelines, p)
}

func (f *FileD) setupInput(p *pipeline.Pipeline, pipelineConfig *cfg.PipelineConfig, values map[string]int) error {
	inputInfo, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindInput, values)
	if err != nil {
		return err
	}

	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo:  inputInfo,
		PluginRuntimeInfo: f.instantiatePlugin(inputInfo),
	})

	for _, actionType := range inputInfo.AdditionalActions {
		actionInfo := f.plugins.GetActionByType(actionType)

		infoCopy := *actionInfo
		infoCopy.Config = inputInfo.Config
		infoCopy.Type = actionType

		p.AddAction(&pipeline.ActionPluginStaticInfo{
			PluginStaticInfo: &infoCopy,
			MatchConditions:  pipeline.MatchConditions{},
		})
	}

	return nil
}

func (f *FileD) setupActions(p *pipeline.Pipeline, pipelineConfig *cfg.PipelineConfig, values map[string]int) {
	actions := pipelineConfig.Raw.Get("actions")
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			logger.Fatalf("empty action #%d for pipeline %q", index, p.Name)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			logger.Fatalf("action #%d doesn't provide type %q", index, p.Name)
		}
		f.setupAction(p, index, t, actionJSON, values)
	}
}

func (f *FileD) setupAction(p *pipeline.Pipeline, index int, t string, actionJSON *simplejson.Json, values map[string]int) {
	logger.Infof("creating action with type %q for pipeline %q", t, p.Name)
	info := f.plugins.GetActionByType(t)

	matchMode, err := extractMatchMode(actionJSON)
	if err != nil {
		logger.Fatalf("can't extract match mode for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	matchInvert, err := extractMatchInvert(actionJSON)
	if err != nil {
		logger.Fatalf("can't extract invert match mode for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		logger.Fatalf("can't extract conditions for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	metricName, metricLabels := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)

	_, config := info.Factory()
	err = json.Unmarshal(configJSON, config)
	if err != nil {
		logger.Fatalf("can't unmarshal config for %s action in pipeline %q: %s", info.Type, p.Name, err.Error())
	}

	err = cfg.Parse(config, values)
	if err != nil {
		logger.Fatalf("wrong config for %q action in pipeline %q: %s", info.Type, p.Name, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config
	infoCopy.Type = t

	p.AddAction(&pipeline.ActionPluginStaticInfo{
		PluginStaticInfo: &infoCopy,
		MatchConditions:  conditions,
		MatchMode:        matchMode,
		MetricName:       metricName,
		MetricLabels:     metricLabels,
		MatchInvert:      matchInvert,
	})
}

func (f *FileD) setupOutput(p *pipeline.Pipeline, pipelineConfig *cfg.PipelineConfig, values map[string]int) error {
	info, err := f.getStaticInfo(pipelineConfig, pipeline.PluginKindOutput, values)
	if err != nil {
		return err
	}

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo:  info,
		PluginRuntimeInfo: f.instantiatePlugin(info),
	})

	return nil
}

func (f *FileD) instantiatePlugin(info *pipeline.PluginStaticInfo) *pipeline.PluginRuntimeInfo {
	plugin, _ := info.Factory()
	return &pipeline.PluginRuntimeInfo{
		Plugin: plugin,
		ID:     "",
	}
}

func (f *FileD) getStaticInfo(pipelineConfig *cfg.PipelineConfig, pluginKind pipeline.PluginKind, values map[string]int) (*pipeline.PluginStaticInfo, error) {
	configJSON := pipelineConfig.Raw.Get(string(pluginKind))
	if configJSON.MustMap() == nil {
		return nil, fmt.Errorf("no %s plugin provided", pluginKind)
	}
	t := configJSON.Get("type").MustString()
	if t == "" {
		return nil, fmt.Errorf("%s doesn't have type", pluginKind)
	}

	logger.Infof("creating %s with type %q", pluginKind, t)
	info := f.plugins.Get(pluginKind, t)
	configJson, err := configJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for %s", t)
	}

	_, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal config for %s: %s", pluginKind, err.Error())
	}

	err = cfg.Parse(config, values)
	if err != nil {
		logger.Fatalf("wrong config for %q plugin %q: %s", pluginKind, t, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config

	return &infoCopy, nil
}

func (f *FileD) Stop() {
	logger.Infof("stopping pipelines=%d", len(f.Pipelines))
	_ = f.server.Shutdown(context.TODO())
	for _, p := range f.Pipelines {
		p.Stop()
	}
}

func (f *FileD) startHTTP() {
	if f.httpAddr == "off" {
		return
	}

	mux := http.DefaultServeMux

	mux.HandleFunc("/live", f.serveLiveReady)
	mux.HandleFunc("/ready", f.serveLiveReady)
	mux.HandleFunc("/freeosmem", f.serveFreeOsMem)
	mux.Handle("/metrics", promhttp.Handler())

	f.server = &http.Server{Addr: f.httpAddr, Handler: mux}
	longpanic.Go(f.listenHTTP)
}

func (f *FileD) listenHTTP() {
	err := f.server.ListenAndServe()
	if err != nil {
		logger.Fatalf("http listening error address=%q: %s", f.httpAddr, err.Error())
	}
}

func (f *FileD) serveFreeOsMem(_ http.ResponseWriter, _ *http.Request) {
	debug.FreeOSMemory()
	logger.Infof("free OS memory OK")
}

func (f *FileD) serveLiveReady(_ http.ResponseWriter, _ *http.Request) {
	logger.Infof("live/ready OK")
}
