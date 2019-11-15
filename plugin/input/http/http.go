package http

import (
	"io"
	"net/http"
	"sync"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Config struct {
	Address string `json:"address"`
}

type Plugin struct {
	config     *Config
	params     *pipeline.InputPluginParams
	readBuffs  *sync.Pool
	eventBuffs *sync.Pool
	controller pipeline.InputPluginController
	server     *http.Server
}

func init() {
	filed.DefaultPluginRegistry.RegisterInput(&pipeline.PluginInfo{
		Type:    "http",
		Factory: Factory,
	})
}

func Factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.config = config.(*Config)
	p.params = params
	p.readBuffs = &sync.Pool{
		New: p.newReadBuff,
	}

	p.eventBuffs = &sync.Pool{
		New: p.newEventBuffs,
	}

	p.controller = params.Controller

	if p.config.Address != "off" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", p.serve)
		p.server = &http.Server{Addr: p.config.Address, Handler: mux}

		go p.listenHTTP()
	}
}

func (p *Plugin) listenHTTP() {
	err := p.server.ListenAndServe()
	if err != nil {
		logger.Fatalf("input plugin http listening error address=%q: %s", p.config.Address, err.Error())
	}
}

func (p *Plugin) newReadBuff() interface{} {
	return make([]byte, 16*1024)
}

func (p *Plugin) newEventBuffs() interface{} {
	return make([]byte, 0, p.params.PipelineSettings.AvgLogSize)
}

func (p *Plugin) serve(w http.ResponseWriter, r *http.Request) {
	readBuff := p.readBuffs.Get().([]byte)
	eventBuff := p.eventBuffs.Get().([]byte)[:0]

	for {
		n, err := r.Body.Read(readBuff)
		if n == 0 && err == io.EOF {
			break
		}
		if err != nil && err != io.EOF {
			logger.Errorf("http input read error: %s", err.Error())
			break
		}

		eventBuff = p.processChunk(readBuff[:n], eventBuff)
	}

	p.readBuffs.Put(readBuff)
	p.eventBuffs.Put(eventBuff)
}

func (p *Plugin) processChunk(readBuff []byte, eventBuff []byte) []byte {
	pos := 0
	nlPos := 0
	for pos < len(readBuff) {
		if readBuff[pos] != '\n' {
			pos++
			continue
		}

		if len(eventBuff) != 0 {
			eventBuff = append(eventBuff, readBuff[:pos]...)
			p.controller.In(0, "", 0, eventBuff)
			eventBuff = eventBuff[:0]
		} else {
			p.controller.In(0, "", 0, readBuff[nlPos:pos])
		}

		pos++
		nlPos = pos
	}

	eventBuff = append(eventBuff, readBuff[nlPos:]...)

	return eventBuff
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(event *pipeline.Event) {
}
