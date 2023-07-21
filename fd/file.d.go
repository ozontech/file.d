package fd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"runtime"
	"runtime/debug"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/buildinfo"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/atomic"
)

type FileD struct {
	config    *cfg.Config
	httpAddr  string
	registry  *prometheus.Registry
	plugins   *PluginRegistry
	Pipelines []*pipeline.Pipeline
	server    *http.Server
	mux       *http.ServeMux
	metricCtl *metric.Ctl

	// file_d metrics

	versionMetric *prometheus.CounterVec
}

func New(config *cfg.Config, httpAddr string) *FileD {
	return &FileD{
		config:    config,
		httpAddr:  httpAddr,
		mux:       http.NewServeMux(),
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
	f.initMetrics()
	f.startHTTP()
	f.startPipelines()
}

func (f *FileD) initMetrics() {
	f.metricCtl = metric.New("file_d", f.registry)
	f.versionMetric = f.metricCtl.RegisterCounter("version", "", "version")
	f.versionMetric.WithLabelValues(buildinfo.Version).Inc()
}

func (f *FileD) createRegistry() {
	f.registry = prometheus.NewRegistry()
	f.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f.registry.MustRegister(prometheus.NewGoCollector())
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
	mux := f.mux
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

	matchMode := extractMatchMode(actionJSON)
	if matchMode == pipeline.MatchModeUnknown {
		logger.Fatalf("unknown match_mode value for action %d/%s in pipeline %q", index, t, p.Name)
	}
	matchInvert := extractMatchInvert(actionJSON)
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		logger.Fatalf("can't extract conditions for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
	}
	metricName, metricLabels, skipStatus := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)

	_, config := info.Factory()
	if err := DecodeConfig(config, configJSON); err != nil {
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
		MetricSkipStatus: skipStatus,
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
	// delete for success decode into config
	configJSON.Del("type")
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
	if err := DecodeConfig(config, configJson); err != nil {
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

func DecodeConfig(config pipeline.AnyConfig, configJson []byte) error {
	dec := json.NewDecoder(bytes.NewReader(configJson))
	dec.DisallowUnknownFields()
	return dec.Decode(config)
}

func (f *FileD) Stop(ctx context.Context) error {
	logger.Infof("stopping pipelines=%d", len(f.Pipelines))
	var err error
	if f.server != nil {
		err = f.server.Shutdown(ctx)
	}
	for _, p := range f.Pipelines {
		p.Stop()
	}

	return err
}

func (f *FileD) startHTTP() {
	if f.httpAddr == "off" {
		return
	}

	mux := f.mux

	// pprof handlers
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	mux.HandleFunc("/live", f.serveLiveReady)
	mux.HandleFunc("/ready", f.serveLiveReady)
	mux.HandleFunc("/freeosmem", f.serveFreeOsMem)
	mux.Handle("/metrics", promhttp.InstrumentMetricHandler(
		f.registry, promhttp.HandlerFor(f.registry, promhttp.HandlerOpts{}),
	))
	mux.Handle("/log/level", logger.Level)

	// serve value changers to set runtime values
	mux.Handle("/runtime/mutex-profile-fraction", valueChangerHandler{
		changeValue: func(n int) {
			runtime.SetMutexProfileFraction(n)
		},
		getValue: func() int {
			return runtime.SetMutexProfileFraction(-1)
		},
	})
	oldBlockProfileRate := atomic.Int64{}
	mux.Handle("/runtime/block-profile-rate", valueChangerHandler{
		changeValue: func(n int) {
			oldBlockProfileRate.Store(int64(n))
			runtime.SetBlockProfileRate(n)
		},
		getValue: func() int {
			return int(oldBlockProfileRate.Load())
		},
	})

	f.server = &http.Server{Addr: f.httpAddr, Handler: mux}
	go f.listenHTTP()
}

func (f *FileD) listenHTTP() {
	err := f.server.ListenAndServe()
	if err != http.ErrServerClosed {
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

type valueChangerHandler struct {
	changeValue func(n int)
	getValue    func() int
}

var _ http.Handler = valueChangerHandler{}

func (h valueChangerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func(body io.ReadCloser) {
		_ = body.Close()
	}(r.Body)

	switch r.Method {
	case http.MethodPut:
		req := struct {
			Value int `json:"value"`
		}{}

		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("decode request: %s", err), http.StatusBadRequest)
			return
		}

		h.changeValue(req.Value)

		// return current value
		fallthrough
	case http.MethodGet:
		res := struct {
			Value int `json:"value"`
		}{
			Value: h.getValue(),
		}

		_ = json.NewEncoder(w).Encode(res)
	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}
