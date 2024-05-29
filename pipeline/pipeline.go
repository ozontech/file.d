package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline/antispam"
	"github.com/ozontech/file.d/pipeline/metadata"
	"github.com/prometheus/client_golang/prometheus"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	DefaultStreamField         = "stream"
	DefaultCapacity            = 1024
	DefaultAvgInputEventSize   = 4 * 1024
	DefaultMaxInputEventSize   = 0
	DefaultJSONNodePoolSize    = 1024
	DefaultMaintenanceInterval = time.Second * 5
	DefaultEventTimeout        = time.Second * 30
	DefaultFieldValue          = "not_set"
	DefaultStreamName          = StreamName("not_set")
	DefaultMetricHoldDuration  = time.Minute * 30

	EventSeqIDError = uint64(0)

	antispamUnbanIterations = 4
)

type finalizeFn = func(event *Event, notifyInput bool, backEvent bool)

type InputPluginController interface {
	In(sourceID SourceID, sourceName string, offset int64, data []byte, isNewSource bool, meta metadata.MetaData) uint64
	UseSpread()                    // don't use stream field and spread all events across all processors
	DisableStreams()               // don't use stream field
	SuggestDecoder(t decoder.Type) // set decoder type if pipeline uses "auto" value for decoder
	IncReadOps()                   // inc read ops for metric
	IncMaxEventSizeExceeded()      // inc max event size exceeded counter
}

type ActionPluginController interface {
	Propagate(event *Event) // throw held event back to pipeline
	Spawn(parent *Event, nodes []*insaneJSON.Node)
	IncMaxEventSizeExceeded() // inc max event size exceeded counter
}

type OutputPluginController interface {
	Commit(event *Event) // notify input plugin that event is successfully processed and save offsets
	Error(err string)
}

type (
	SourceID   uint64
	StreamName string
)

type Pipeline struct {
	Name     string
	started  bool
	settings *Settings

	decoderType          decoder.Type // decoder type set in the config
	suggestedDecoderType decoder.Type // decoder type suggested by input plugin, it is used when config decoder is set to "auto"
	decoder              decoder.Decoder

	eventPool *eventPool
	streamer  *streamer

	useSpread      bool
	disableStreams bool
	singleProc     bool
	shouldStop     atomic.Bool

	input      InputPlugin
	inputInfo  *InputPluginInfo
	antispamer *antispam.Antispammer

	actionInfos   []*ActionPluginStaticInfo
	actionMetrics actionMetrics
	actionParams  PluginDefaultParams

	Procs       []*processor
	procCount   *atomic.Int32
	activeProcs *atomic.Int32

	output     OutputPlugin
	outputInfo *OutputPluginInfo

	metricHolder *metric.Holder

	// some debugging stuff
	logger          *zap.Logger
	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex

	inputEvents  atomic.Int64
	inputSize    atomic.Int64
	outputEvents atomic.Int64
	outputSize   atomic.Int64
	readOps      atomic.Int64

	// all pipeline`s metrics
	inUseEventsMetric          prometheus.Gauge
	eventPoolCapacityMetric    prometheus.Gauge
	inputEventsCountMetric     prometheus.Counter
	inputEventSizeMetric       prometheus.Counter
	outputEventsCountMetric    prometheus.Counter
	outputEventSizeMetric      prometheus.Counter
	readOpsEventsSizeMetric    prometheus.Counter
	wrongEventCRIFormatMetric  prometheus.Counter
	maxEventSizeExceededMetric prometheus.Counter
	eventPoolLatency           prometheus.Observer
}

type Settings struct {
	Decoder             string
	DecoderParams       map[string]any
	Capacity            int
	MaintenanceInterval time.Duration
	EventTimeout        time.Duration
	AntispamThreshold   int
	AntispamField       string
	AntispamExceptions  matchrule.RuleSets
	AvgEventSize        int
	MaxEventSize        int
	StreamField         string
	IsStrict            bool
	MetricHoldDuration  time.Duration
}

// New creates new pipeline. Consider using `SetupHTTPHandlers` next.
func New(name string, settings *Settings, registry *prometheus.Registry) *Pipeline {
	metricCtl := metric.NewCtl("pipeline_"+name, registry)

	lg := logger.Instance.Named(name).Desugar()

	pipeline := &Pipeline{
		Name:           name,
		logger:         lg,
		settings:       settings,
		useSpread:      false,
		disableStreams: false,
		actionParams: PluginDefaultParams{
			PipelineName:     name,
			PipelineSettings: settings,
			MetricCtl:        metricCtl,
		},
		actionMetrics: actionMetrics{
			m:  make(map[string]*actionMetric),
			mu: new(sync.RWMutex),
		},
		metricHolder: metric.NewHolder(settings.MetricHoldDuration),
		streamer:     newStreamer(settings.EventTimeout),
		eventPool:    newEventPool(settings.Capacity, settings.AvgEventSize),
		antispamer: antispam.NewAntispammer(&antispam.Options{
			MaintenanceInterval: settings.MaintenanceInterval,
			Threshold:           settings.AntispamThreshold,
			Field:               settings.AntispamField,
			UnbanIterations:     antispamUnbanIterations,
			Logger:              lg.Named("antispam"),
			MetricsController:   metricCtl,
			Exceptions:          settings.AntispamExceptions,
		}),

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},
	}

	pipeline.registerMetrics()
	pipeline.setDefaultMetrics()

	switch settings.Decoder {
	case "json":
		pipeline.decoderType = decoder.JSON
	case "raw":
		pipeline.decoderType = decoder.RAW
	case "cri":
		pipeline.decoderType = decoder.CRI
	case "postgres":
		pipeline.decoderType = decoder.POSTGRES
	case "nginx_error":
		pipeline.decoderType = decoder.NGINX_ERROR
	case "protobuf":
		pipeline.decoderType = decoder.PROTOBUF

		dec, err := decoder.NewProtobufDecoder(pipeline.settings.DecoderParams)
		if err != nil {
			pipeline.logger.Fatal("can't create protobuf decoder", zap.Error(err))
		}
		pipeline.decoder = dec
	case "auto":
		pipeline.decoderType = decoder.AUTO
	default:
		pipeline.logger.Fatal("unknown decoder", zap.String("decoder", settings.Decoder))
	}

	return pipeline
}

func (p *Pipeline) IncReadOps() {
	p.readOps.Inc()
}

func (p *Pipeline) IncMaxEventSizeExceeded() {
	p.maxEventSizeExceededMetric.Inc()
}

func (p *Pipeline) registerMetrics() {
	m := p.actionParams.MetricCtl
	p.inUseEventsMetric = m.RegisterGauge("event_pool_in_use_events", "Count of pool events which is used for processing")
	p.eventPoolCapacityMetric = m.RegisterGauge("event_pool_capacity", "Pool capacity value")
	p.inputEventsCountMetric = m.RegisterCounter("input_events_count", "Count of events on pipeline input")
	p.inputEventSizeMetric = m.RegisterCounter("input_events_size", "Size of events on pipeline input")
	p.outputEventsCountMetric = m.RegisterCounter("output_events_count", "Count of events on pipeline output")
	p.outputEventSizeMetric = m.RegisterCounter("output_events_size", "Size of events on pipeline output")
	p.readOpsEventsSizeMetric = m.RegisterCounter("read_ops_count", "Read OPS count")
	p.wrongEventCRIFormatMetric = m.RegisterCounter("wrong_event_cri_format", "Wrong event CRI format counter")
	p.maxEventSizeExceededMetric = m.RegisterCounter("max_event_size_exceeded", "Max event size exceeded counter")
	p.eventPoolLatency = m.RegisterHistogram("event_pool_latency_seconds",
		"How long we are wait an event from the pool", metric.SecondsBucketsDetailedNano)
}

func (p *Pipeline) setDefaultMetrics() {
	p.eventPoolCapacityMetric.Set(float64(p.settings.Capacity))
}

// SetupHTTPHandlers creates handlers for plugin endpoints and pipeline info.
// Plugin endpoints can be accessed via
// URL `/pipelines/<pipeline_name>/<plugin_index_in_config>/<plugin_endpoint>`.
// Input plugin has the index of zero, output plugin has the last index.
// Actions also have the standard endpoints `/info` and `/sample`.
func (p *Pipeline) SetupHTTPHandlers(mux *http.ServeMux) {
	if p.input == nil {
		p.logger.Panic("input isn't set")
	}
	if p.output == nil {
		p.logger.Panic("output isn't set")
	}

	prefix := "/pipelines/" + p.Name
	mux.HandleFunc(prefix, p.servePipeline)
	prefixBanList := fmt.Sprintf("/pipelines/%s/ban_list", p.Name)
	mux.HandleFunc(prefixBanList, p.servePipelineBanList)
	for hName, handler := range p.inputInfo.PluginStaticInfo.Endpoints {
		mux.HandleFunc(fmt.Sprintf("%s/0/%s", prefix, hName), handler)
	}

	for i, info := range p.actionInfos {
		mux.HandleFunc(fmt.Sprintf("%s/%d/info", prefix, i+1), p.serveActionInfo(info))
		mux.HandleFunc(fmt.Sprintf("%s/%d/sample", prefix, i+1), p.serveActionSample(i))
		for hName, handler := range info.PluginStaticInfo.Endpoints {
			mux.HandleFunc(fmt.Sprintf("%s/%d/%s", prefix, i+1, hName), handler)
		}
	}

	for hName, handler := range p.outputInfo.PluginStaticInfo.Endpoints {
		mux.HandleFunc(fmt.Sprintf("%s/%d/%s", prefix, len(p.actionInfos)+1, hName), handler)
	}
}

func (p *Pipeline) Start() {
	if p.input == nil {
		p.logger.Panic("input isn't set")
	}
	if p.output == nil {
		p.logger.Panic("output isn't set")
	}

	p.initProcs()

	outputParams := &OutputPluginParams{
		PluginDefaultParams: p.actionParams,
		Controller:          p,
		Logger:              p.logger.Sugar().Named("output").Named(p.outputInfo.Type),
	}
	p.logger.Info("starting output plugin", zap.String("name", p.outputInfo.Type))

	p.output.Start(p.outputInfo.Config, outputParams)

	p.logger.Info("stating processors", zap.Int("count", len(p.Procs)))
	for _, processor := range p.Procs {
		processor.start(p.actionParams, p.logger.Sugar())
	}

	p.logger.Info("starting input plugin", zap.String("name", p.inputInfo.Type))
	inputParams := &InputPluginParams{
		PluginDefaultParams: p.actionParams,
		Controller:          p,
		Logger:              p.logger.Sugar().Named("input").Named(p.inputInfo.Type),
	}

	p.input.Start(p.inputInfo.Config, inputParams)

	p.streamer.start()

	go p.maintenance()
	if !p.useSpread {
		go p.growProcs()
	}
	p.started = true
}

func (p *Pipeline) Stop() {
	p.logger.Info("stopping pipeline", zap.Int64("committed", p.outputEvents.Load()))

	p.logger.Info("stopping processors", zap.Int32("count", p.procCount.Load()))
	for _, processor := range p.Procs {
		processor.stop()
	}

	p.streamer.stop()

	p.logger.Info("stopping input")
	p.input.Stop()

	p.logger.Info("stopping output")
	p.output.Stop()

	p.shouldStop.Store(true)
}

func (p *Pipeline) SetInput(info *InputPluginInfo) {
	p.inputInfo = info
	p.input = info.Plugin.(InputPlugin)
}

func (p *Pipeline) GetInput() InputPlugin {
	return p.input
}

func (p *Pipeline) SetOutput(info *OutputPluginInfo) {
	p.outputInfo = info
	p.output = info.Plugin.(OutputPlugin)
}

func (p *Pipeline) GetOutput() OutputPlugin {
	return p.output
}

// In decodes message and passes it to event stream.
func (p *Pipeline) In(sourceID SourceID, sourceName string, offset int64, bytes []byte, isNewSource bool, meta metadata.MetaData) (seqID uint64) {
	length := len(bytes)

	// don't process mud.
	isEmpty := length == 0 || (bytes[0] == '\n' && length == 1)
	if isEmpty {
		return EventSeqIDError
	}

	isLong := p.settings.MaxEventSize != 0 && length > p.settings.MaxEventSize
	if isLong {
		p.IncMaxEventSizeExceeded()
		return EventSeqIDError
	}

	var (
		dec decoder.Type
		row decoder.CRIRow
		err error
	)
	if p.decoderType == decoder.AUTO {
		dec = p.suggestedDecoderType
	} else {
		dec = p.decoderType
	}
	if dec == decoder.NO {
		dec = decoder.JSON
	} else if dec == decoder.CRI {
		row, err = decoder.DecodeCRI(bytes)
		if err != nil {
			p.wrongEventCRIFormatMetric.Inc()
			p.Error(fmt.Sprintf("wrong cri format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes))
			return EventSeqIDError
		}
	}

	// Skip IsSpam for partial logs is necessary to avoid the case
	// when some parts of a large event have got into the ban,
	// thereby cutting off a piece of the event.
	// This is only possible if the event was written in CRI format and has Partial status.
	// For other encoding formats this is not relevant as they always come in full.
	// The event is Partial if it is larger than the driver configuration.
	// For example, for containerd this setting is called max_container_log_line_size
	// https://github.com/containerd/containerd/blob/f7f2be732159a411eae46b78bfdb479b133a823b/pkg/cri/config/config.go#L263-L266
	if !row.IsPartial && p.settings.AntispamThreshold > 0 {
		var checkSourceID any
		var checkSourceName string
		if p.settings.AntispamField == "" {
			checkSourceID = uint64(sourceID)
			checkSourceName = sourceName
		} else {
			if val, ok := meta[p.settings.AntispamField]; ok {
				checkSourceID = val
				checkSourceName = val
				isNewSource = false
			} else {
				p.Error(fmt.Sprintf("antispam_field %s does not exists in meta", p.settings.AntispamField))
			}
		}
		isSpam := p.antispamer.IsSpam(checkSourceID, checkSourceName, isNewSource, bytes)
		if isSpam {
			return EventSeqIDError
		}
	}

	p.inputEvents.Inc()
	p.inputSize.Add(int64(length))

	now := time.Now()
	event := p.eventPool.get()
	p.eventPoolLatency.Observe(time.Since(now).Seconds())

	switch dec {
	case decoder.JSON:
		err := event.parseJSON(bytes)
		if err != nil {
			level := zapcore.ErrorLevel
			if p.settings.IsStrict {
				level = zapcore.FatalLevel
			}

			p.logger.Log(level, "wrong json format", zap.Error(err),
				zap.Int64("offset", offset),
				zap.Int("length", length),
				zap.Uint64("source", uint64(sourceID)),
				zap.String("source_name", sourceName),
				zap.ByteString("json", bytes))

			// Can't process event, return to pool.
			p.eventPool.back(event)
			return EventSeqIDError
		}
	case decoder.RAW:
		_ = event.Root.DecodeString("{}")
		event.Root.AddFieldNoAlloc(event.Root, "message").MutateToBytesCopy(event.Root, bytes[:len(bytes)-1])
	case decoder.CRI:
		_ = event.Root.DecodeString("{}")
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToBytesCopy(event.Root, row.Log)
		event.Root.AddFieldNoAlloc(event.Root, "time").MutateToBytesCopy(event.Root, row.Time)
		event.Root.AddFieldNoAlloc(event.Root, "stream").MutateToBytesCopy(event.Root, row.Stream)
	case decoder.POSTGRES:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodePostgres(event.Root, bytes)
		if err != nil {
			p.logger.Fatal("wrong postgres format", zap.Error(err),
				zap.Int64("offset", offset),
				zap.Int("length", length),
				zap.Uint64("source", uint64(sourceID)),
				zap.String("source_name", sourceName),
				zap.ByteString("log", bytes))

			// Dead route, never passed here.
			return EventSeqIDError
		}
	case decoder.NGINX_ERROR:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodeNginxError(event.Root, bytes)
		if err != nil {
			level := zapcore.ErrorLevel
			if p.settings.IsStrict {
				level = zapcore.FatalLevel
			}

			p.logger.Log(level, "wrong nginx error log format", zap.Error(err),
				zap.Int64("offset", offset),
				zap.Int("length", length),
				zap.Uint64("source", uint64(sourceID)),
				zap.String("source_name", sourceName),
				zap.ByteString("log", bytes))

			p.eventPool.back(event)
			return EventSeqIDError
		}
	case decoder.PROTOBUF:
		_ = event.Root.DecodeString("{}")
		err = p.decoder.Decode(event.Root, bytes)
		if err != nil {
			p.logger.Fatal("wrong protobuf format", zap.Error(err),
				zap.Int64("offset", offset),
				zap.Int("length", length),
				zap.Uint64("source", uint64(sourceID)),
				zap.String("source_name", sourceName),
				zap.ByteString("log", bytes))

			// Dead route, never passed here.
			return EventSeqIDError
		}
	default:
		p.logger.Panic("unknown decoder", zap.Int("decoder", int(dec)))
	}

	if len(meta) > 0 {
		if event.Root.IsArray() {
			nodeArray := event.Root.AsArray()
			for _, elem := range nodeArray {
				if elem.IsObject() {
					for k, v := range meta {
						elem.AddField(k).MutateToString(v)
					}
				}
			}
		} else {
			for k, v := range meta {
				CreateNestedField(event.Root, []string{k}).MutateToString(v)
			}
		}
	}

	event.Offset = offset
	event.SourceID = sourceID
	event.SourceName = sourceName
	event.streamName = DefaultStreamName
	event.Size = len(bytes)

	return p.streamEvent(event)
}

func (p *Pipeline) streamEvent(event *Event) uint64 {
	streamID := StreamID(event.SourceID)

	// spread events across all processors
	if p.useSpread {
		streamID = StreamID(event.SeqID % uint64(p.procCount.Load()))
	}

	if !p.disableStreams {
		node := event.Root.Dig(p.settings.StreamField)
		if node != nil {
			event.streamName = StreamName(node.AsString())
		}

		if pass := p.input.PassEvent(event); !pass {
			// Can't process event, return to pool.
			p.eventPool.back(event)
			return EventSeqIDError
		}
	}

	return p.streamer.putEvent(streamID, event.streamName, event)
}

func (p *Pipeline) Commit(event *Event) {
	p.finalize(event, true, true)
}

func (p *Pipeline) Error(err string) {
	if p.settings.IsStrict {
		logger.Fatal(err)
	} else {
		logger.Error(err)
	}
}

func (p *Pipeline) finalize(event *Event, notifyInput bool, backEvent bool) {
	if event.IsTimeoutKind() || event.IsChildKind() {
		return
	}

	if notifyInput {
		p.input.Commit(event)
		p.outputEvents.Inc()
		p.outputSize.Add(int64(event.Size))
	}

	// todo: avoid event.stream.commit(event)
	event.stream.commit(event)

	if !backEvent {
		return
	}

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, event.Root.EncodeToString())
		p.eventLogMu.Unlock()
	}

	for _, e := range event.children {
		insaneJSON.Release(e.Root)
	}
	p.eventPool.back(event)
}

type actionMetric struct {
	count metric.HeldCounterVec
	size  metric.HeldCounterVec
	// totalCounter is a map of eventStatus to counter for `/info` endpoint.
	totalCounter map[string]*atomic.Uint64
}

type actionMetrics struct {
	m  map[string]*actionMetric
	mu *sync.RWMutex
}

func (am *actionMetrics) set(name string, m *actionMetric) {
	if name == "" {
		return
	}

	am.mu.Lock()
	am.m[name] = m
	am.mu.Unlock()
}

func (am *actionMetrics) get(name string) *actionMetric {
	if name == "" {
		return nil
	}

	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.m[name]
}

func (p *Pipeline) AddAction(info *ActionPluginStaticInfo) {
	p.actionInfos = append(p.actionInfos, info)

	mCtl := p.actionParams.MetricCtl

	labels := make([]string, 0, len(info.MetricLabels)+1)
	if !info.MetricSkipStatus {
		labels = append(labels, "status")
	}
	labels = append(labels, info.MetricLabels...)

	count := mCtl.RegisterCounterVec(
		info.MetricName+"_events_count_total",
		fmt.Sprintf("how many events processed by pipeline %q and #%d action", p.Name, len(p.actionInfos)-1),
		labels...,
	)
	heldCount := p.metricHolder.AddCounterVec(count)

	size := mCtl.RegisterCounterVec(
		info.MetricName+"_events_size_total",
		fmt.Sprintf("total size of events processed by pipeline %q and #%d action", p.Name, len(p.actionInfos)-1),
		labels...,
	)
	heldSize := p.metricHolder.AddCounterVec(size)

	totalCounter := make(map[string]*atomic.Uint64)
	for _, st := range allEventStatuses() {
		totalCounter[string(st)] = atomic.NewUint64(0)
	}

	p.actionMetrics.set(info.MetricName, &actionMetric{
		count:        heldCount,
		size:         heldSize,
		totalCounter: totalCounter,
	})
}

func (p *Pipeline) initProcs() {
	// default proc count is CPU cores * 2
	procCount := runtime.GOMAXPROCS(0) * 2
	if p.singleProc {
		procCount = 1
	}
	p.logger.Info("starting pipeline", zap.Int("procs", procCount))

	p.procCount = atomic.NewInt32(int32(procCount))
	p.activeProcs = atomic.NewInt32(0)

	p.Procs = make([]*processor, 0, procCount)
	for i := 0; i < procCount; i++ {
		p.Procs = append(p.Procs, p.newProc(i))
	}
}

func (p *Pipeline) newProc(id int) *processor {
	proc := newProcessor(
		id,
		&p.actionMetrics,
		p.activeProcs,
		p.output,
		p.streamer,
		p.finalize,
		p.IncMaxEventSizeExceeded,
	)
	for j, info := range p.actionInfos {
		plugin, _ := info.Factory()
		proc.AddActionPlugin(&ActionPluginInfo{
			ActionPluginStaticInfo: info,
			PluginRuntimeInfo: &PluginRuntimeInfo{
				Plugin: plugin,
				ID:     strconv.Itoa(proc.id) + "_" + strconv.Itoa(j),
			},
		})
	}

	return proc
}

func (p *Pipeline) growProcs() {
	interval := time.Millisecond * 100
	t := time.Now()
	for {
		time.Sleep(interval)
		if p.shouldStop.Load() {
			return
		}
		if p.procCount.Load() != p.activeProcs.Load() {
			continue
		}

		if time.Since(t) > interval {
			p.expandProcs()
			t = time.Now()
		}
	}
}

func (p *Pipeline) expandProcs() {
	if p.singleProc {
		return
	}

	from := p.procCount.Load()
	to := from * 2
	p.logger.Info("processors count expanded from %d to %d", zap.Int32("old", from), zap.Int32("new", to))
	if to > 10000 {
		p.logger.Warn("too many processors", zap.Int32("new", to))
	}

	// proc IDs are added starting from the next after the last one
	// so all procs have unique IDs
	for x := 1; x <= int(to-from); x++ {
		proc := p.newProc(p.Procs[from-1].id + x)
		p.Procs = append(p.Procs, proc)
		proc.start(p.actionParams, p.logger.Sugar())
	}

	p.procCount.Swap(to)
}

type deltas struct {
	deltaInputEvents  float64
	deltaInputSize    float64
	deltaOutputEvents float64
	deltaOutputSize   float64
	deltaReads        float64
}

func (p *Pipeline) logChanges(myDeltas *deltas) {
	if ce := p.logger.Check(zapcore.InfoLevel, "pipeline stats"); ce != nil {
		inputSize := p.inputSize.Load()
		inputEvents := p.inputEvents.Load()
		inUseEvents := p.eventPool.inUseEvents.Load()

		interval := p.settings.MaintenanceInterval
		rate := int(myDeltas.deltaInputEvents * float64(time.Second) / float64(interval))
		rateMb := myDeltas.deltaInputSize * float64(time.Second) / float64(interval) / 1024 / 1024
		readOps := int(myDeltas.deltaReads * float64(time.Second) / float64(interval))
		tc := int64(math.Max(float64(inputSize), 1))

		stat := fmt.Sprintf(`interval=%ds, active procs=%d/%d, events in use=%d/%d, out=%d|%.1fMb,`+
			` rate=%d/s|%.1fMb/s, read ops=%d/s, total=%d|%.1fMb, avg size=%d`,
			interval/time.Second, p.activeProcs.Load(), p.procCount.Load(),
			inUseEvents, p.settings.Capacity,
			int64(myDeltas.deltaInputEvents), myDeltas.deltaInputSize/1024.0/1024.0, rate, rateMb, readOps,
			inputEvents, float64(inputSize)/1024.0/1024.0, inputSize/tc)

		ce.Write(zap.String("stat", stat))
	}
}

func (p *Pipeline) incMetrics(inputEvents, inputSize, outputEvents, outputSize, reads *DeltaWrapper) *deltas {
	deltaInputEvents := inputEvents.updateValue(p.inputEvents.Load())
	deltaInputSize := inputSize.updateValue(p.inputSize.Load())
	deltaOutputEvents := outputEvents.updateValue(p.outputEvents.Load())
	deltaOutputSize := outputSize.updateValue(p.outputSize.Load())
	deltaReads := reads.updateValue(p.readOps.Load())

	myDeltas := &deltas{
		deltaInputEvents,
		deltaInputSize,
		deltaOutputEvents,
		deltaOutputSize,
		deltaReads,
	}

	p.inputEventsCountMetric.Add(myDeltas.deltaInputEvents)
	p.inputEventSizeMetric.Add(myDeltas.deltaInputSize)
	p.outputEventsCountMetric.Add(myDeltas.deltaOutputEvents)
	p.outputEventSizeMetric.Add(myDeltas.deltaOutputSize)
	p.readOpsEventsSizeMetric.Add(myDeltas.deltaReads)

	return myDeltas
}

func (p *Pipeline) setMetrics(inUseEvents int64) {
	p.inUseEventsMetric.Set(float64(inUseEvents))
}

func (p *Pipeline) maintenance() {
	inputEvents := newDeltaWrapper()
	inputSize := newDeltaWrapper()
	outputEvents := newDeltaWrapper()
	outputSize := newDeltaWrapper()
	readOps := newDeltaWrapper()

	for {
		time.Sleep(p.settings.MaintenanceInterval)
		if p.shouldStop.Load() {
			return
		}

		p.antispamer.Maintenance()
		p.metricHolder.Maintenance()

		myDeltas := p.incMetrics(inputEvents, inputSize, outputEvents, outputSize, readOps)
		p.setMetrics(p.eventPool.inUseEvents.Load())
		p.logChanges(myDeltas)
	}
}

func (p *Pipeline) UseSpread() {
	if p.started {
		p.logger.Panic("don't use (*Pipeline).UseSpread after the pipeline has started")
	}
	p.useSpread = true
}

func (p *Pipeline) DisableStreams() {
	p.disableStreams = true
}

func (p *Pipeline) SuggestDecoder(t decoder.Type) {
	p.suggestedDecoderType = t
}

func (p *Pipeline) DisableParallelism() {
	p.singleProc = true
}

func (p *Pipeline) GetEventsTotal() int {
	return int(p.outputEvents.Load())
}

func (p *Pipeline) EnableEventLog() {
	p.eventLogEnabled = true
}

func (p *Pipeline) GetEventLogItem(index int) string {
	p.eventLogMu.Lock()
	defer p.eventLogMu.Unlock()

	if index >= len(p.eventLog) {
		p.logger.Fatal("can't find log item", zap.Int("index", index))
	}
	return p.eventLog[index]
}

func (p *Pipeline) servePipeline(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("<html><body><pre><p>"))
	_, _ = w.Write([]byte(logger.Header("pipeline " + p.Name)))
	_, _ = w.Write([]byte(p.streamer.dump()))
	_, _ = w.Write([]byte(p.eventPool.dump()))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}

func (p *Pipeline) servePipelineBanList(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("<html><body><pre><p>"))
	_, _ = w.Write([]byte(logger.Header("pipeline " + p.Name)))
	_, _ = w.Write([]byte(p.antispamer.Dump()))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}

// serveActionInfo creates a handlerFunc for the given action.
// it returns metric values for the given action.
func (p *Pipeline) serveActionInfo(info *ActionPluginStaticInfo) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Type", "application/json")

		type Event struct {
			Status string `json:"status"`
			Count  int    `json:"count"`
		}

		if info.MetricName == "" {
			w.WriteHeader(http.StatusBadRequest)
			writeErr(w, "If you want to see a statistic about events, consider adding `metric_name` to the action's configuration.")

			return
		}

		am := p.actionMetrics.get(info.MetricName)

		var events []Event
		for _, status := range []eventStatus{
			eventStatusReceived,
			eventStatusDiscarded,
			eventStatusPassed,
		} {
			c := am.totalCounter[string(status)]
			if c == nil {
				c = atomic.NewUint64(0)
			}
			events = append(events, Event{
				Status: string(status),
				Count:  int(c.Load()),
			})
		}

		resp, _ := json.Marshal(events)
		_, _ = w.Write(resp)
	}
}

// serveActionSample creates a handlerFunc for the given action.
// The func watch every processor, store their events before and after processing,
// and returns the first result from the fastest processor.
func (p *Pipeline) serveActionSample(actionIndex int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Type", "application/json")

		if p.activeProcs.Load() <= 0 || p.procCount.Load() <= 0 {
			w.WriteHeader(http.StatusBadRequest)
			writeErr(w, "There are no active processors")

			return
		}

		timeout := 5 * time.Second

		samples := make(chan sample, len(p.Procs))
		for _, proc := range p.Procs {
			go func(proc *processor) {
				if sample, err := proc.actionWatcher.watch(actionIndex, timeout); err == nil {
					samples <- *sample
				}
			}(proc)
		}

		select {
		case firstSample := <-samples:
			_, _ = w.Write(firstSample.Marshal())
		case <-time.After(timeout):
			w.WriteHeader(http.StatusInternalServerError)
			writeErr(w, "Timeout while try to display an event before and after the action processing.")
		}
	}
}

func writeErr(w io.Writer, err string) {
	type ErrResp struct {
		Error string `json:"error"`
	}

	respErr, _ := json.Marshal(ErrResp{
		Error: err,
	})
	_, _ = w.Write(respErr)
}
