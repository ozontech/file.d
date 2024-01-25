package http

import (
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/ozontech/file.d/xtls"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
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
```bash
# run server.
# config.yaml should contains yaml config above.
go run ./cmd/file.d --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'
```
}*/

const (
	readBufDefaultLen = 16 * 1024
)

type Plugin struct {
	mu sync.Mutex

	server            *http.Server
	config            *Config
	nameByBearerToken map[string]string
	logger            *zap.Logger

	params     *pipeline.InputPluginParams
	controller pipeline.InputPluginController

	sourceIDs []pipeline.SourceID
	sourceSeq pipeline.SourceID

	gzipReaderPool sync.Pool
	readBuffs      sync.Pool
	eventBuffs     sync.Pool

	// plugin metrics

	successfulAuthTotal   map[string]prometheus.Counter
	failedAuthTotal       prometheus.Counter
	errorsTotal           prometheus.Counter
	bulkRequestsDoneTotal prometheus.Counter
	requestsInProgress    prometheus.Gauge
	processBulkSeconds    prometheus.Observer

	metaTemplater *metadata.MetaTemplater
}

type EmulateMode byte

const (
	EmulateModeNo EmulateMode = iota
	EmulateModeElasticSearch
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > An address to listen to. Omit ip/host to listen all network interfaces. E.g. `:88`
	Address string `json:"address" default:":9200"` // *
	// > @3@4@5@6
	// >
	// > Which protocol to emulate.
	EmulateMode  string `json:"emulate_mode" default:"no" options:"no|elasticsearch"` // *
	EmulateMode_ EmulateMode
	// > @3@4@5@6
	// >
	// > CA certificate in PEM encoding. This can be a path or the content of the certificate.
	// > If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.
	CACert string `json:"ca_cert" default:""` // *
	// > @3@4@5@6
	// >
	// > CA private key in PEM encoding. This can be a path or the content of the key.
	// > If both ca_cert and private_key are set, the server starts accepting connections in TLS mode.
	PrivateKey string `json:"private_key" default:""` // *

	// > @3@4@5@6
	// >
	// > Auth config.
	// > Disabled by default.
	// > See AuthConfig for details.
	// > You can use 'warn' log level for logging authorizations.
	Auth AuthConfig `json:"auth" child:"true"` // *

	// > @3@4@5@6
	// >
	// > Meta params
	// >
	// > Add meta information to an event (look at Meta params)
	// > Use [go-template](https://pkg.go.dev/text/template) syntax
	// >
	// > Example: ```user_agent: '{{ index (index .request.Header "User-Agent") 0}}'```
	Meta cfg.MetaTemplates `json:"meta"` // *
}

type AuthStrategy byte

const (
	StrategyDisabled AuthStrategy = iota
	StrategyBasic
	StrategyBearer
)

// ! config-params
// ^ config-params
type AuthConfig struct {
	// > @3@4@5@6
	// >
	// > Override default Authorization header
	Header string `json:"header" default:"Authorization"` // *

	// > @3@4@5@6
	// >
	// > AuthStrategy.Strategy describes strategy to use.
	Strategy  string `json:"strategy" default:"disabled" options:"disabled|basic|bearer"` // *
	Strategy_ AuthStrategy
	// > @3@4@5@6
	// >
	// > AuthStrategy.Secrets describes secrets in key-value format.
	// > If the `strategy` is basic, then the key is the login, the value is the password.
	// > If the `strategy` is bearer, then the key is the name, the value is the Bearer token.
	// > Key uses in the http_input_total metric.
	Secrets map[string]string `json:"secrets"` // *
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
	p.logger = params.Logger.Desugar()
	p.registerMetrics(params.MetricCtl)
	p.metaTemplater = metadata.NewMetaTemplater(p.config.Meta)

	if p.config.Auth.Strategy_ == StrategyBearer {
		p.nameByBearerToken = make(map[string]string, len(p.config.Auth.Secrets))
		for name, token := range p.config.Auth.Secrets {
			p.nameByBearerToken[token] = name
		}
	}

	p.controller = params.Controller
	p.controller.DisableStreams()
	p.sourceIDs = make([]pipeline.SourceID, 0)

	p.server = &http.Server{
		Addr:    p.config.Address,
		Handler: http.Handler(p),
	}

	if p.config.Address != "off" {
		go p.listenHTTP()
	}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.bulkRequestsDoneTotal = ctl.RegisterCounter("bulk_requests_done_total", "")
	p.requestsInProgress = ctl.RegisterGauge("requests_in_progress", "")
	p.processBulkSeconds = ctl.RegisterHistogram("process_bulk_seconds", "", metric.SecondsBucketsDetailed)
	p.errorsTotal = ctl.RegisterCounter("input_http_errors", "Total http errors")

	if p.config.Auth.Strategy_ != StrategyDisabled {
		httpAuthTotal := ctl.RegisterCounterVec("http_auth_success_total", "", "secret_name")
		p.successfulAuthTotal = make(map[string]prometheus.Counter, len(p.config.Auth.Secrets))
		for key := range p.config.Auth.Secrets {
			p.successfulAuthTotal[key] = httpAuthTotal.WithLabelValues(key)
		}
		p.failedAuthTotal = ctl.RegisterCounter("http_auth_fails_total", "")
	}
}

func (p *Plugin) listenHTTP() {
	var err error
	if p.config.CACert != "" || p.config.PrivateKey != "" {
		tlsBuilder := xtls.NewConfigBuilder()
		err = tlsBuilder.AppendX509KeyPair(p.config.CACert, p.config.PrivateKey)
		if err == nil {
			p.server.TLSConfig = tlsBuilder.Build()
			err = p.server.ListenAndServeTLS("", "")
		}
	} else {
		err = p.server.ListenAndServe()
	}

	if err != nil {
		p.logger.Fatal("input plugin http listening error", zap.String("addr", p.config.Address), zap.Error(err))
	}
}

func (p *Plugin) newReadBuff() []byte {
	if buff := p.readBuffs.Get(); buff != nil {
		return *buff.(*[]byte)
	}
	return make([]byte, readBufDefaultLen)
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

func (p *Plugin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ok, login := p.auth(r)

	if !ok {
		p.failedAuthTotal.Inc()
		p.errorsTotal.Inc()
		p.logger.Warn("auth failed",
			zap.String("user_agent", r.UserAgent()),
			zap.Any("headers", r.Header),
			zap.String("remote_addr", r.RemoteAddr),
		)
		http.Error(w, "auth failed", http.StatusUnauthorized)
		return
	}

	var metadataInfo metadata.MetaData
	var err error
	if len(p.config.Meta) > 0 {
		metadataInfo, err = p.metaTemplater.Render(newMetaInformation(login, getUserIP(r), r))
		if err != nil {
			p.logger.Error("cannot parse meta info", zap.Error(err))
		}
	}

	path := r.URL.Path
	switch p.config.EmulateMode_ {
	case EmulateModeElasticSearch:
		w.Header().Add("Content-Type", "application/json")
		w.Header().Set("X-Elastic-Product", "Elasticsearch")

		switch path {
		case "/_bulk":
			p.serveBulk(w, r, metadataInfo)
			return
		case "/":
			p.serveElasticsearchInfo(w, r)
			return
		case "/_xpack":
			p.serveElasticsearchXPack(w, r)
			return
		case "/_license":
			p.serveElasticsearchLicense(w, r)
			return
		}

		if strings.HasPrefix(path, "/_ilm/policy") {
			_, _ = w.Write(empty)
			return
		}
		if strings.HasPrefix(path, "/_index_template") {
			_, _ = w.Write(empty)
			return
		}
		if strings.HasPrefix(path, "/_template") {
			_, _ = w.Write(empty)
			return
		}
		if strings.HasPrefix(path, "/_ingest") {
			_, _ = w.Write(empty)
			return
		}
		if strings.HasPrefix(path, "/_nodes") {
			_, _ = w.Write(empty)
			return
		}

		p.logger.Error("unknown elasticsearch request", zap.String("uri", r.RequestURI), zap.String("method", r.Method))
		return
	case EmulateModeNo:
		p.serveBulk(w, r, metadataInfo)
		return
	default:
		panic("unreachable")
	}
}

func (p *Plugin) serveBulk(w http.ResponseWriter, r *http.Request, meta metadata.MetaData) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()
	p.requestsInProgress.Inc()

	reader := io.Reader(r.Body)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := p.acquireGzipReader(reader)
		if err != nil {
			p.errorsTotal.Inc()
			p.logger.Error("can't read gzipped body", zap.Error(err))
			http.Error(w, "can't read gzipped body", http.StatusBadRequest)
			return
		}
		defer p.putGzipReader(zr)
		reader = zr
	}

	if err := p.processBulk(reader, meta); err != nil {
		p.errorsTotal.Inc()
		p.logger.Error("http input read error", zap.Error(err))
		http.Error(w, "http input read error", http.StatusBadRequest)
		return
	}

	_, _ = w.Write(result)

	p.requestsInProgress.Dec()
	p.bulkRequestsDoneTotal.Inc()
	p.processBulkSeconds.Observe(time.Since(start).Seconds())
}

func (p *Plugin) processBulk(r io.Reader, meta metadata.MetaData) error {
	readBuff := p.newReadBuff()
	eventBuff := p.newEventBuffs()
	defer p.readBuffs.Put(&readBuff)
	defer p.eventBuffs.Put(&eventBuff)

	sourceID := p.getSourceID()
	defer p.putSourceID(sourceID)

	for {
		n, err := r.Read(readBuff)
		if n == 0 && err == io.EOF {
			break
		}

		if err != nil && err != io.EOF {
			return err
		}

		eventBuff = p.processChunk(sourceID, readBuff[:n], eventBuff, false, meta)
	}

	if len(eventBuff) > 0 {
		eventBuff = p.processChunk(sourceID, readBuff[:0], eventBuff, true, meta)
	}

	return nil
}

func (p *Plugin) processChunk(sourceID pipeline.SourceID, readBuff []byte, eventBuff []byte, isLastChunk bool, meta metadata.MetaData) []byte {
	pos := 0   // current position
	nlPos := 0 // new line position
	for pos < len(readBuff) {
		if readBuff[pos] != '\n' {
			pos++
			continue
		}

		if len(eventBuff) != 0 {
			eventBuff = append(eventBuff, readBuff[nlPos:pos]...)
			_ = p.controller.In(sourceID, "http", int64(pos), eventBuff, true, meta)
			eventBuff = eventBuff[:0]
		} else {
			_ = p.controller.In(sourceID, "http", int64(pos), readBuff[nlPos:pos], true, meta)
		}

		pos++
		nlPos = pos
	}

	if isLastChunk {
		// flush buffers if we can't find the newline character
		_ = p.controller.In(sourceID, "http", int64(pos), append(eventBuff, readBuff[nlPos:]...), true, meta)
		eventBuff = eventBuff[:0]
	} else {
		eventBuff = append(eventBuff, readBuff[nlPos:]...)
	}

	return eventBuff
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Commit(_ *pipeline.Event) {
	// todo: don't reply with OK till all events in request will be committed
}

// PassEvent decides pass or discard event.
func (p *Plugin) PassEvent(_ *pipeline.Event) bool {
	return true
}

func (p *Plugin) auth(req *http.Request) (bool, string) {
	if p.config.Auth.Strategy_ == StrategyDisabled {
		return true, ""
	}

	var secretName string
	var ok bool
	switch p.config.Auth.Strategy_ {
	case StrategyBasic:
		secretName, ok = p.authBasic(req)
	case StrategyBearer:
		secretName, ok = p.authBearer(req)
	default:
		panic("unreachable")
	}
	if !ok {
		return false, ""
	}
	p.successfulAuthTotal[secretName].Inc()
	return true, secretName
}

func (p *Plugin) authBasic(req *http.Request) (string, bool) {
	req.Header.Get(p.config.Auth.Header)
	req.Header.Set("Authorization", req.Header.Get(p.config.Auth.Header))

	username, password, ok := req.BasicAuth()
	if !ok {
		return username, false
	}
	return username, p.config.Auth.Secrets[username] == password
}

func (p *Plugin) authBearer(req *http.Request) (string, bool) {
	authHeader := req.Header.Get(p.config.Auth.Header)
	const prefix = "Bearer "
	if !strings.HasPrefix(authHeader, prefix) {
		return "", false
	}
	token := authHeader[len(prefix):]
	name, ok := p.nameByBearerToken[token]
	return name, ok
}

func (p *Plugin) acquireGzipReader(r io.Reader) (*gzip.Reader, error) {
	anyReader := p.gzipReaderPool.Get()
	if anyReader == nil {
		return gzip.NewReader(r)
	}
	gzReader := anyReader.(*gzip.Reader)
	err := gzReader.Reset(r)
	return gzReader, err
}

func (p *Plugin) putGzipReader(reader *gzip.Reader) {
	_ = reader.Close()
	p.gzipReaderPool.Put(reader)
}

func getUserIP(r *http.Request) net.IP {
	var userIP string
	switch {
	case r.Header.Get("CF-Connecting-IP") != "":
		userIP = r.Header.Get("CF-Connecting-IP")
	case r.Header.Get("X-Forwarded-For") != "":
		userIP = r.Header.Get("X-Forwarded-For")
	case r.Header.Get("X-Real-IP") != "":
		userIP = r.Header.Get("X-Real-IP")
	default:
		userIP = r.RemoteAddr
		if strings.Contains(userIP, ":") {
			return net.ParseIP(strings.Split(userIP, ":")[0])
		}
	}
	return net.ParseIP(userIP)
}

type metaInformation struct {
	login      string
	remoteAddr net.IP
	request    *http.Request
}

func newMetaInformation(login string, ip net.IP, r *http.Request) metaInformation {
	return metaInformation{
		login:      login,
		remoteAddr: ip,
		request:    r,
	}
}

func (m metaInformation) GetData() map[string]any {
	return map[string]any{
		"login":       m.login,
		"remote_addr": m.remoteAddr,
		"request":     m.request,
	}
}
