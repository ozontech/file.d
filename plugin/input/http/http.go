package http

import (
	"io"
	"net/http"
	"sync"

	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
Reads events from HTTP requests with body delimited by a new line.

Also it emulates some protocols to allow receive events from wide range of software which use HTTP to transmit data.
E.g. `file.d` may pretends to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it have read all the request body.
> It doesn't wait until events will be committed.
}*/

type Plugin struct {
	config     *Config
	params     *pipeline.InputPluginParams
	readBuffs  *sync.Pool
	eventBuffs *sync.Pool
	controller pipeline.InputPluginController
	server     *http.Server
	sourceIDs  []pipeline.SourceID
	sourceSeq  pipeline.SourceID
	mu         *sync.Mutex
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6   
	//> 
	//> Address to listen to. Omit ip/host to listen for all network interfaces. E.g. `:88`
	Address string `json:"address" default:":9200"` //*
	//> @3@4@5@6
	//>
	//> Which protocol to emulate.
	EmulateMode string `json:"emulate_mode" default:"no" options:"no|elasticsearch"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterInput(&pipeline.PluginStaticInfo{
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

	p.mu = &sync.Mutex{}
	p.controller = params.Controller
	p.sourceIDs = make([]pipeline.SourceID, 0, 0)

	mux := http.NewServeMux()
	switch p.config.EmulateMode {
	case "elasticsearch":
		p.elasticsearch(mux)
	case "no":
		mux.HandleFunc("/", p.serve)
	}
	p.server = &http.Server{Addr: p.config.Address, Handler: mux}

	if p.config.Address != "off" {
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

func (p *Plugin) getSourceID() pipeline.SourceID {
	p.mu.Lock()
	if len(p.sourceIDs) == 0 {
		p.sourceIDs = append(p.sourceIDs, p.sourceSeq)
		p.sourceSeq++
	}

	l := len(p.sourceIDs)
	x := p.sourceIDs[l-1]
	p.sourceIDs = p.sourceIDs[:l-1]
	p.mu.Unlock()

	return x
}

func (p *Plugin) putSourceID(x pipeline.SourceID) {
	p.mu.Lock()
	p.sourceIDs = append(p.sourceIDs, x)
	p.mu.Unlock()
}

func (p *Plugin) serve(w http.ResponseWriter, r *http.Request) {
	readBuff := p.readBuffs.Get().([]byte)
	eventBuff := p.eventBuffs.Get().([]byte)[:0]

	sourceID := p.getSourceID()
	defer p.putSourceID(sourceID)

	for {
		n, err := r.Body.Read(readBuff)
		if n == 0 && err == io.EOF {
			break
		}

		if err != nil && err != io.EOF {
			logger.Errorf("http input read error: %s", err.Error())
			break
		}

		eventBuff = p.processChunk(sourceID, readBuff[:n], eventBuff)
	}

	p.readBuffs.Put(readBuff)
	p.eventBuffs.Put(eventBuff)

	_, err := w.Write(result)
	if err != nil {
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) processChunk(sourceID pipeline.SourceID, readBuff []byte, eventBuff []byte) []byte {
	pos := 0
	nlPos := 0
	for pos < len(readBuff) {
		if readBuff[pos] != '\n' {
			pos++
			continue
		}

		if len(eventBuff) != 0 {
			eventBuff = append(eventBuff, readBuff[nlPos:pos]...)
			p.controller.In(sourceID, "http", int64(pos), eventBuff, true)
			eventBuff = eventBuff[:0]
		} else {
			p.controller.In(sourceID, "http", int64(pos), readBuff[nlPos:pos], true)
		}

		pos++
		nlPos = pos
	}

	eventBuff = append(eventBuff, readBuff[nlPos:]...)

	return eventBuff
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(_ *pipeline.Event) {
	//todo: don't reply with OK till all events in request will be committed
}
