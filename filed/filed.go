package filed

import (
	"encoding/json"
	"net/http"
	"runtime"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Filed struct {
	config         *Config
	pluginRegistry *PluginRegistry
	pipelines      []*pipeline.SplitPipeline
}

func New(config *Config) *Filed {
	return &Filed{
		config:         config,
		pluginRegistry: DefaultPluginRegistry,
	}
}

func NewWithPluginRegistry(config *Config, pluginRegistry *PluginRegistry) *Filed {
	return &Filed{
		config:         config,
		pluginRegistry: pluginRegistry,
	}
}

func (f *Filed) SetConfig(config *Config) {
	f.config = config
}

func (f *Filed) Start() {
	logger.Infof("starting filed")

	go f.startLiveReadyHTTPEndpoint()

	f.pipelines = f.pipelines[:0]
	for name, config := range f.config.pipelines {
		procs := runtime.GOMAXPROCS(0)
		tracksCount := procs * 4
		headsCount := procs * 4

		logger.Infof("starting pipeline %q using %d processor cores", name, procs)

		p := pipeline.New(name, tracksCount, headsCount)

		f.startInput(config, name, p)
		f.startActions(config, name, p)

		p.Start()

		f.pipelines = append(f.pipelines, p)
	}
}

func (f *Filed) startInput(pipelineConfig *PipelineConfig, pipelineName string, p *pipeline.SplitPipeline) {
	input := pipelineConfig.raw.Get("input")
	if input.MustMap() == nil {
		logger.Fatalf("no input for pipeline %q", pipelineName)
	}
	t := input.Get("type").MustString()
	if t == "" {
		logger.Fatalf("no input type provided for pipeline %q", pipelineName)
	}

	logger.Infof("creating input with type %q", t)
	info := f.pluginRegistry.GetInputByType(t)
	configJson, err := input.Encode()
	if err != nil {
		logger.Panicf("can't create config json for input %q in pipeline %q", t, pipelineName)
	}

	plugin, config := info.Factory()
	err = json.Unmarshal(configJson, config)
	if err != nil {
		logger.Panicf("can't unmarshal config for input %q in pipeline %q", t, pipelineName)
	}

	p.SetInputPlugin(&pipeline.PluginDescription{Plugin: plugin, Config: config})
}

func (f *Filed) startActions(pipelineConfig *PipelineConfig, pipelineName string, p *pipeline.SplitPipeline) {
	actions := pipelineConfig.raw.Get("actions")
	for index := range actions.MustArray() {
		action := actions.GetIndex(index)
		if action.MustMap() == nil {
			logger.Fatalf("empty action #%d for pipeline %q", index, pipelineName)
		}

		t := action.Get("type").MustString()
		if t == "" {
			logger.Fatalf("action #%d doesn't provide type for pipeline %q", index, pipelineName)
		}

		logger.Infof("creating action with type %q", t)
		info := f.pluginRegistry.GetActionByType(t)
		configJson, err := action.Encode()
		if err != nil {
			logger.Panicf("can't create config json for action #%d in pipeline %q", index, t, pipelineName)
		}

		for _, track := range p.Tracks {
			plugin, config := info.Factory()
			err = json.Unmarshal(configJson, config)
			if err != nil {
				logger.Panicf("can't unmarshal config for action #%d in pipeline %q", index, t, pipelineName)
			}
			track.AddActionPlugin(&pipeline.PluginDescription{Plugin: plugin, Config: config})
		}
	}
}

func (f *Filed) Stop() {
	for _, p := range f.pipelines {
		p.Stop()
	}
}

func (f *Filed) startLiveReadyHTTPEndpoint() {
	addr := ":9000"
	mux := http.DefaultServeMux
	liveReadyHandler := &liveReadyHandler{}
	mux.Handle("/live", liveReadyHandler)
	mux.Handle("/ready", liveReadyHandler)

	err := http.ListenAndServe(addr, mux)
	if err != nil {
		logger.Fatalf("can't start live/ready http endpoint at %q: %s", addr, err.Error())
	}
}

type liveReadyHandler struct {
}

func (p *liveReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.Info("Live/ready OK")
}
