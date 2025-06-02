package fd

import (
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
	insaneJSON "github.com/ozontech/insane-json"
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
	f.metricCtl = metric.NewCtl("file_d", f.registry)
	f.versionMetric = f.metricCtl.RegisterCounterVec("version", "", "version")
	f.versionMetric.WithLabelValues(buildinfo.Version).Inc()
}

func (f *FileD) createRegistry() {
	f.registry = prometheus.NewRegistry()
	f.registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	f.registry.MustRegister(prometheus.NewGoCollector())
	f.registry.MustRegister(newFdsCollector())
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

	if settings.Pool == pipeline.PoolTypeLowMem {
		insaneJSON.StartNodePoolSize = 16
	}
	p := pipeline.New(name, settings, f.registry, logger.Instance.Named(name).Desugar())
	err := f.setupInput(p, config, values)
	if err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

	actions := config.Raw.Get("actions")
	if err := SetupActions(p, f.plugins, actions, values); err != nil {
		logger.Fatalf("can't create pipeline %q: %s", name, err.Error())
	}

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
		actionInfo, err := f.plugins.GetActionByType(actionType)
		if err != nil {
			return err
		}

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

func SetupActions(p *pipeline.Pipeline, plugins *PluginRegistry, actions *simplejson.Json, values map[string]int) error {
	for index := range actions.MustArray() {
		actionJSON := actions.GetIndex(index)
		if actionJSON.MustMap() == nil {
			return fmt.Errorf("empty action #%d for pipeline %q", index, p.Name)
		}

		t := actionJSON.Get("type").MustString()
		if t == "" {
			return fmt.Errorf("action #%d doesn't provide type %q", index, p.Name)
		}
		if err := setupAction(p, plugins, index, t, actionJSON, values); err != nil {
			return err
		}
	}
	return nil
}

func setupAction(p *pipeline.Pipeline, plugins *PluginRegistry, index int, t string, actionJSON *simplejson.Json, values map[string]int) error {
	info, err := plugins.GetActionByType(t)
	if err != nil {
		return err
	}

	doIfChecker, err := extractDoIfChecker(actionJSON.Get("do_if"))
	if err != nil {
		logger.Fatalf(`failed to extract "do_if" conditions for action %d/%s in pipeline %q: %s`, index, t, p.Name, err.Error())
	}

	matchMode := extractMatchMode(actionJSON)
	if matchMode == pipeline.MatchModeUnknown {
		return fmt.Errorf("unknown match_mode value for action %d/%s", index, t)
	}
	matchInvert := extractMatchInvert(actionJSON)
	conditions, err := extractConditions(actionJSON.Get("match_fields"))
	if err != nil {
		return fmt.Errorf("can't extract conditions for action %d/%s: %s", index, t, err.Error())
	}
	metricName, metricLabels, skipStatus := extractMetrics(actionJSON)
	configJSON := makeActionJSON(actionJSON)
	config, err := pipeline.GetConfig(info, configJSON, values)
	if err != nil {
		return fmt.Errorf("wrong config for action %d/%s in pipeline %q: %s", index, t, p.Name, err.Error())
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
		DoIfChecker:      doIfChecker,
	})
	return nil
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
	info, err := f.plugins.Get(pluginKind, t)
	if err != nil {
		return nil, err
	}
	configJson, err := configJSON.Encode()
	if err != nil {
		logger.Panicf("can't create config json for %s", t)
	}
	config, err := pipeline.GetConfig(info, configJson, values)
	if err != nil {
		logger.Fatalf("error on creating %s with type %q: %s", t, pluginKind, err.Error())
	}

	infoCopy := *info
	infoCopy.Config = config

	return &infoCopy, nil
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
