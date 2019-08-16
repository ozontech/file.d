package filed

import (
	"net/http"

	"gitlab.ozon.ru/sre/filed/logger"
)

type Filed struct {
	config         *Config
	pluginRegistry *PluginRegistry
	done           chan bool
}

func New(config *Config) *Filed {
	return &Filed{
		config:         config,
		pluginRegistry: DefaultPluginRegistry,
		done:           make(chan bool),
	}
}

func NewWithPluginRegistry(config *Config, pluginRegistry *PluginRegistry) *Filed {
	return &Filed{
		config:         config,
		pluginRegistry: pluginRegistry,
		done:           make(chan bool),
	}
}

func (f *Filed) SetConfig(config *Config) {
	f.config = config
}

func (f *Filed) Start()  {
	logger.Info("starting filed")
	go f.startLiveReadyHTTPEndpoint()
	logger.Info("filed started")

	<-f.done
	return
}

func (f *Filed) Stop() {
	// todo: stop logic here
	f.done <- true
}

func (f *Filed) startLiveReadyHTTPEndpoint() {
	addr := ":9000"
	logger.Infof("starting live/ready http endpoint at %q", addr)

	mux := http.DefaultServeMux
	liveReadyHandler := &liveReadyHandler{}
	mux.Handle("/live", liveReadyHandler)
	mux.Handle("/ready", liveReadyHandler)

	err := http.ListenAndServe(addr, mux)
	if err != nil {
		logger.Panicf("can't start live/ready http endpoint at %q: %s", addr, err.Error())
	}
}

type liveReadyHandler struct {
}

func (p *liveReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.Info("Live/ready OK")
}
