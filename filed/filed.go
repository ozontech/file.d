package filed

import (
	"gitlab.ozon.ru/sre/filed/config"
	"gitlab.ozon.ru/sre/filed/global"
	"net/http"
)

type Filed struct {
	config         *config.Config
	pluginRegistry *PluginRegistry
}

type Plugin interface {
	Start()
}

func New(config *config.Config, pluginRegistry *PluginRegistry) *Filed {
	return &Filed{
		config:         config,
		pluginRegistry: pluginRegistry,
	}
}

func (f *Filed) Start() error {
	global.Logger.Info("starting filed")
	go f.startLiveReadyHTTPEndpoint()
	global.Logger.Info("filed started")

	done := make(chan bool)

	<-done
	return nil
}

func (f *Filed) startLiveReadyHTTPEndpoint() {
	addr := ":9000"
	global.Logger.Infof("starting live/ready http endpoint at %q", addr)

	mux := http.DefaultServeMux
	liveReadyHandler := &liveReadyHandler{}
	mux.Handle("/live", liveReadyHandler)
	mux.Handle("/ready", liveReadyHandler)

	err := http.ListenAndServe(addr, mux)
	if err != nil {
		global.Logger.Panicf("can't start live/ready http endpoint at %q: %s", addr, err.Error())
	}
}

type liveReadyHandler struct {
}

func (p *liveReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	global.Logger.Info("Live/ready OK")
}
