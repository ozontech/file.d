package filed

import (
	"encoding/json"
	"net/http"

	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Filed struct {
	config         *Config
	pluginRegistry *PluginRegistry
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

	for pipelineName, pipelineConfig := range f.config.pipelines {
		logger.Infof("starting pipeline %q", pipelineName)

		controller := pipeline.NewController(false)

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

		controller.SetInputPlugin(&pipeline.PluginWithConfig{plugin, config})
		controller.Start()
	}

	return
}

func (f *Filed) Stop() {
	// todo: stop logic here
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
