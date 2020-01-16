package http

import (
	"io"
	"net/http"
	"sync"

	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
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
	sourceIDs  []pipeline.SourceID
	sourceSeq  pipeline.SourceID
	mu         *sync.Mutex
}

var info = []byte(`{
  "name" : "filed_elasticsearch_input",
  "cluster_name" : "filed",
  "cluster_uuid" : "Rz-wj_pkT8a0Y1KXTLmN9g",
  "version" : {
    "number" : "6.7.1",
    "build_flavor" : "default",
    "build_type" : "deb",
    "build_hash" : "2f32220",
    "build_date" : "2019-04-02T15:59:27.961366Z",
    "build_snapshot" : false,
    "lucene_version" : "7.7.0",
    "minimum_wire_compatibility_version" : "5.6.0",
    "minimum_index_compatibility_version" : "5.0.0"
  },
  "tagline" : "You know, for file.d"
}`)

var xpack = []byte(`{
  "build": {
    "date": "2019-04-02T15:59:27.961366Z",
    "hash": "2f32220"
  },
  "features": {
    "graph": {
      "available": false,
      "description": "Graph Data Exploration for the Elastic Stack",
      "enabled": true
    },
    "ilm": {
      "available": true,
      "description": "Index lifecycle management for the Elastic Stack",
      "enabled": true
    },
    "logstash": {
      "available": false,
      "description": "Logstash management component for X-Pack",
      "enabled": true
    },
    "ml": {
      "available": false,
      "description": "Machine Learning for the Elastic Stack",
      "enabled": false,
      "native_code_info": {
        "build_hash": "N/A",
        "version": "N/A"
      }
    },
    "monitoring": {
      "available": true,
      "description": "Monitoring for the Elastic Stack",
      "enabled": true
    },
    "rollup": {
      "available": true,
      "description": "Time series pre-aggregation and rollup",
      "enabled": true
    },
    "security": {
      "available": false,
      "description": "Security for the Elastic Stack",
      "enabled": false
    },
    "sql": {
      "available": true,
      "description": "SQL access to Elasticsearch",
      "enabled": true
    },
    "watcher": {
      "available": false,
      "description": "Alerting, Notification and Automation for the Elastic Stack",
      "enabled": true
    }
  },
  "license": {
    "mode": "basic",
    "status": "active",
    "type": "basic",
    "uid": "e76d6ce9-f78c-44ff-8fd5-b5877357d649"
  },
  "tagline": "You know, for nothing"
}`)

var result = []byte(`{
   "took": 30,
   "errors": false,
   "items": []
}`)

var empty = []byte(`{}`)

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

	if p.config.Address != "off" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", p.serveInfo)
		mux.HandleFunc("/_xpack", p.serveXPack)
		mux.HandleFunc("/_bulk", p.serveBulk)
		mux.HandleFunc("/_template/", p.serveTemplate)
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

func (p *Plugin) serveXPack(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write(xpack)
	if err != nil {
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) serveTemplate(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write(empty)
	if err != nil {
		logger.Errorf("can't write response: %s", err.Error())
	}
}

func (p *Plugin) serveInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.RequestURI == "/" {

		_, err := w.Write(info)
		if err != nil {
			logger.Errorf("can't write response: %s", err.Error())
		}
		return
	}

	logger.Errorf("unknown request uri=%s, method=%s", r.RequestURI, r.Method)
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

func (p *Plugin) serveBulk(w http.ResponseWriter, r *http.Request) {
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
			p.controller.In(sourceID, "http", int64(pos), eventBuff)
			eventBuff = eventBuff[:0]
		} else {
			p.controller.In(sourceID, "http", int64(pos), readBuff[nlPos:pos])
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
