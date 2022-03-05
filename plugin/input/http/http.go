package http

import (
	"io"
	"net/http"
	"sync"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HTTP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

**Example:**
Emulating elastic through http:
```yaml
pipelines:
  example_k8s_pipeline:
    settings:
      capacity: 1024
    input:
      # define input type.
      type: http
      # pretend elastic search, emulate it's protocol.
      emulate_mode: "elasticsearch"
      # define http port.
      address: ":9200"
    actions:
      # parse elastic search query.
      - type: parse_es
      # decode elastic search json.
      - type: json_decode
        # field is required.
        field: message
    output:
      # Let's write to kafka example.
      type: kafka
        brokers: [kafka-local:9092, kafka-local:9091]
        default_topic: yourtopic-k8s-data
        use_topic_field: true
        topic_field: pipeline_kafka_topic

      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```
# run server.
# config.yaml should contains yaml config above.
go run cmd/file.d.go --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'

##

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
	//> An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`
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
	p.readBuffs = &sync.Pool{}
	p.eventBuffs = &sync.Pool{}
	p.mu = &sync.Mutex{}
	p.controller = params.Controller
	p.controller.DisableStreams()
	p.sourceIDs = make([]pipeline.SourceID, 0)

	mux := http.NewServeMux()
	switch p.config.EmulateMode {
	case "elasticsearch":
		p.elasticsearch(mux)
	case "no":
		mux.HandleFunc("/", p.serve)
	}
	p.server = &http.Server{Addr: p.config.Address, Handler: mux}

	if p.config.Address != "off" {
		longpanic.Go(p.listenHTTP)
	}
}

func (p *Plugin) listenHTTP() {
	err := p.server.ListenAndServe()
	if err != nil {
		logger.Fatalf("input plugin http listening error address=%q: %s", p.config.Address, err.Error())
	}
}

func (p *Plugin) newReadBuff() []byte {
	if buff := p.readBuffs.Get(); buff != nil {
		return *buff.(*[]byte)
	}
	return make([]byte, 16*1024)
}

func (p *Plugin) newEventBuffs() []byte {
	if buff := p.eventBuffs.Get(); buff != nil {
		return (*buff.(*[]byte))[:0]
	}
	return make([]byte, 0, p.params.PipelineSettings.AvgEventSize)
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
	readBuff := p.newReadBuff()
	eventBuff := p.newEventBuffs()

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

	_ = r.Body.Close()

	// https://staticcheck.io/docs/checks/#SA6002
	p.readBuffs.Put(&readBuff)
	p.eventBuffs.Put(&eventBuff)

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
	// todo: don't reply with OK till all events in request will be committed
}
