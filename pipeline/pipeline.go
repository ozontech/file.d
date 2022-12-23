package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/longpanic"
	"github.com/ozontech/file.d/metric"
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

	EventSeqIDError = uint64(0)

	antispamUnbanIterations = 4
	metricsGenInterval      = time.Hour
)

type finalizeFn = func(event *Event, notifyInput bool, backEvent bool)

type InputPluginController interface {
	In(sourceID SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64
	UseSpread()                           // don't use stream field and spread all events across all processors
	DisableStreams()                      // don't use stream field
	SuggestDecoder(t decoder.DecoderType) // set decoder if pipeline uses "auto" value for decoder
	IncReadOps()                          // inc read ops for metric
	IncMaxEventSizeExceeded()             // inc max event size exceeded counter
}

type ActionPluginController interface {
	Commit(event *Event)    // commit offset of held event and skip further processing
	Propagate(event *Event) // throw held event back to pipeline
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

	decoder          decoder.DecoderType // decoder set in the config
	suggestedDecoder decoder.DecoderType // decoder suggested by input plugin, it is used when config decoder is set to "auto"

	eventPool *eventPool
	streamer  *streamer

	useSpread      bool
	disableStreams bool
	singleProc     bool
	shouldStop     bool

	input      InputPlugin
	inputInfo  *InputPluginInfo
	antispamer *antispamer

	actionInfos  []*ActionPluginStaticInfo
	Procs        []*processor
	procCount    *atomic.Int32
	activeProcs  *atomic.Int32
	actionParams *PluginDefaultParams

	output     OutputPlugin
	outputInfo *OutputPluginInfo

	metricsHolder *metricsHolder
	metricsCtl    *metric.Ctl

	// some debugging stuff
	logger          *zap.SugaredLogger
	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex
	inSample        []byte
	outSample       []byte
	inputEvents     atomic.Int64
	inputSize       atomic.Int64
	outputEvents    atomic.Int64
	outputSize      atomic.Int64
	readOps         atomic.Int64
	maxSize         int

	//all pipeline`s metrics

	inUseEventsMetric          *prometheus.GaugeVec
	eventPoolCapacityMetric    *prometheus.GaugeVec
	inputEventsCountMetric     *prometheus.CounterVec
	inputEventSizeMetric       *prometheus.CounterVec
	outputEventsCountMetric    *prometheus.CounterVec
	outputEventSizeMetric      *prometheus.CounterVec
	readOpsEventsSizeMetric    *prometheus.CounterVec
	wrongEventCRIFormatMetric  *prometheus.CounterVec
	maxEventSizeExceededMetric *prometheus.CounterVec
}

type Settings struct {
	Decoder             string
	Capacity            int
	MaintenanceInterval time.Duration
	EventTimeout        time.Duration
	AntispamThreshold   int
	AvgEventSize        int
	MaxEventSize        int
	StreamField         string
	IsStrict            bool
}

// New creates new pipeline. Consider using `SetupHTTPHandlers` next.
func New(name string, settings *Settings, registry *prometheus.Registry) *Pipeline {
	metricCtl := metric.New("pipeline_"+name, registry)

	pipeline := &Pipeline{
		Name:           name,
		logger:         logger.Instance.Named(name),
		settings:       settings,
		useSpread:      false,
		disableStreams: false,
		actionParams: &PluginDefaultParams{
			PipelineName:     name,
			PipelineSettings: settings,
		},

		metricsHolder: newMetricsHolder(name, registry, metricsGenInterval),
		metricsCtl:    metricCtl,
		streamer:      newStreamer(settings.EventTimeout),
		eventPool:     newEventPool(settings.Capacity, settings.AvgEventSize),
		antispamer:    newAntispamer(settings.AntispamThreshold, antispamUnbanIterations, settings.MaintenanceInterval, metricCtl),

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},
	}

	pipeline.registerMetrics()
	pipeline.setDefaultMetrics()

	switch settings.Decoder {
	case "json":
		pipeline.decoder = decoder.JSON
	case "raw":
		pipeline.decoder = decoder.RAW
	case "cri":
		pipeline.decoder = decoder.CRI
	case "postgres":
		pipeline.decoder = decoder.POSTGRES
	case "nginx_error":
		pipeline.decoder = decoder.NGINX_ERROR
	case "auto":
		pipeline.decoder = decoder.AUTO
	default:
		pipeline.logger.Fatalf("unknown decoder %q for pipeline %q", settings.Decoder, name)
	}

	return pipeline
}

func (p *Pipeline) IncReadOps() {
	p.readOps.Inc()
}

func (p *Pipeline) IncMaxEventSizeExceeded() {
	p.maxEventSizeExceededMetric.WithLabelValues().Inc()
}

func (p *Pipeline) registerMetrics() {
	p.inUseEventsMetric = p.metricsCtl.RegisterGauge("event_pool_in_use_events", "Count of pool events which is used for processing")
	p.eventPoolCapacityMetric = p.metricsCtl.RegisterGauge("event_pool_capacity", "Pool capacity value")
	p.inputEventsCountMetric = p.metricsCtl.RegisterCounter("input_events_count", "Count of events on pipeline input")
	p.inputEventSizeMetric = p.metricsCtl.RegisterCounter("input_events_size", "Size of events on pipeline input")
	p.outputEventsCountMetric = p.metricsCtl.RegisterCounter("output_events_count", "Count of events on pipeline output")
	p.outputEventSizeMetric = p.metricsCtl.RegisterCounter("output_events_size", "Size of events on pipeline output")
	p.readOpsEventsSizeMetric = p.metricsCtl.RegisterCounter("read_ops_count", "Read OPS count")
	p.wrongEventCRIFormatMetric = p.metricsCtl.RegisterCounter("wrong_event_cri_format", "Wrong event CRI format counter")
	p.maxEventSizeExceededMetric = p.metricsCtl.RegisterCounter("max_event_size_exceeded", "Max event size exceeded counter")
}

func (p *Pipeline) setDefaultMetrics() {
	p.eventPoolCapacityMetric.WithLabelValues().Set(float64(p.settings.Capacity))
}

// SetupHTTPHandlers creates handlers for plugin endpoints and pipeline info.
// Plugin endpoints can be accessed via
// URL `/pipelines/<pipeline_name>/<plugin_index_in_config>/<plugin_endpoint>`.
// Input plugin has the index of zero, output plugin has the last index.
// Actions also have the standard endpoints `/info` and `/sample`.
func (p *Pipeline) SetupHTTPHandlers(mux *http.ServeMux) {
	if p.input == nil {
		p.logger.Panicf("input isn't set for pipeline %q", p.Name)
	}
	if p.output == nil {
		p.logger.Panicf("output isn't set for pipeline %q", p.Name)
	}

	prefix := "/pipelines/" + p.Name
	mux.HandleFunc(prefix, p.servePipeline)
	prefixBanList := fmt.Sprintf("/pipelines/%s/ban_list", p.Name)
	mux.HandleFunc(prefixBanList, p.servePipelineBanList)
	for hName, handler := range p.inputInfo.PluginStaticInfo.Endpoints {
		mux.HandleFunc(fmt.Sprintf("%s/0/%s", prefix, hName), handler)
	}

	for i, info := range p.actionInfos {
		mux.HandleFunc(fmt.Sprintf("%s/%d/info", prefix, i+1), p.serveActionInfo(*info))
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
		p.logger.Panicf("input isn't set for pipeline %q", p.Name)
	}
	if p.output == nil {
		p.logger.Panicf("output isn't set for pipeline %q", p.Name)
	}

	p.initProcs()
	p.metricsHolder.start()

	outputParams := &OutputPluginParams{
		PluginDefaultParams: p.actionParams,
		Controller:          p,
		Logger:              p.logger.Named("output " + p.outputInfo.Type),
	}
	p.logger.Infof("starting output plugin %q", p.outputInfo.Type)

	p.output.RegisterMetrics(p.metricsCtl)
	p.output.Start(p.outputInfo.Config, outputParams)

	p.logger.Infof("stating processors, count=%d", len(p.Procs))
	for _, processor := range p.Procs {
		processor.registerMetrics(p.metricsCtl)
		processor.start(p.actionParams, p.logger)
	}

	p.logger.Infof("starting input plugin %q", p.inputInfo.Type)
	inputParams := &InputPluginParams{
		PluginDefaultParams: p.actionParams,
		Controller:          p,
		Logger:              p.logger.Named("input " + p.inputInfo.Type),
	}

	p.input.RegisterMetrics(p.metricsCtl)
	p.input.Start(p.inputInfo.Config, inputParams)

	p.streamer.start()

	longpanic.Go(p.maintenance)
	if !p.useSpread {
		longpanic.Go(p.growProcs)
	}
	p.started = true
}

func (p *Pipeline) Stop() {
	p.logger.Infof("stopping pipeline %q, total committed=%d", p.Name, p.outputEvents.Load())

	p.logger.Infof("stopping processors count=%d", len(p.Procs))
	for _, processor := range p.Procs {
		processor.stop()
	}

	p.streamer.stop()

	p.logger.Infof("stopping %q input", p.Name)
	p.input.Stop()

	p.logger.Infof("stopping %q output", p.Name)
	p.output.Stop()

	p.shouldStop = true
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
func (p *Pipeline) In(sourceID SourceID, sourceName string, offset int64, bytes []byte, isNewSource bool) (seqID uint64) {
	length := len(bytes)

	// don't process mud.
	isEmpty := length == 0 || (bytes[0] == '\n' && length == 1)
	isSpam := p.antispamer.isSpam(sourceID, sourceName, isNewSource)
	isLong := p.settings.MaxEventSize != 0 && length > p.settings.MaxEventSize

	if isLong {
		p.IncMaxEventSizeExceeded()
	}
	if isEmpty || isSpam || isLong {
		return EventSeqIDError
	}

	p.inputEvents.Inc()
	p.inputSize.Add(int64(length))

	event := p.eventPool.get()

	var dec decoder.DecoderType
	if p.decoder == decoder.AUTO {
		dec = p.suggestedDecoder
	} else {
		dec = p.decoder
	}
	if dec == decoder.NO {
		dec = decoder.JSON
	}

	switch dec {
	case decoder.JSON:
		err := event.parseJSON(bytes)
		if err != nil {
			if p.settings.IsStrict {
				p.logger.Fatalf("wrong json format offset=%d, length=%d, err=%s, source=%d:%s, json=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			} else {
				p.logger.Errorf("wrong json format offset=%d, length=%d, err=%s, source=%d:%s, json=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			}
			// Can't process event, return to pool.
			p.eventPool.back(event)
			return EventSeqIDError
		}
	case decoder.RAW:
		_ = event.Root.DecodeString("{}")
		event.Root.AddFieldNoAlloc(event.Root, "message").MutateToBytesCopy(event.Root, bytes[:len(bytes)-1])
	case decoder.CRI:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodeCRI(event.Root, bytes)
		if err != nil {
			p.wrongEventCRIFormatMetric.WithLabelValues().Inc()
			if p.settings.IsStrict {
				p.logger.Fatalf("wrong cri format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			} else {
				p.logger.Errorf("wrong cri format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			}
			p.eventPool.back(event)
			return EventSeqIDError
		}
	case decoder.POSTGRES:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodePostgres(event.Root, bytes)
		if err != nil {
			p.logger.Fatalf("wrong postgres format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			// Dead route, never passed here.
			return EventSeqIDError
		}
	case decoder.NGINX_ERROR:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodeNginxError(event.Root, bytes)
		if err != nil {
			if p.settings.IsStrict {
				p.logger.Fatalf("wrong nginx error log format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			} else {
				p.logger.Errorf("wrong nginx error log format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			}
			p.eventPool.back(event)
			return EventSeqIDError
		}
	default:
		p.logger.Panicf("unknown decoder %d for pipeline %q", p.decoder, p.Name)
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

	if len(p.inSample) == 0 {
		p.inSample = event.Root.Encode(p.inSample)
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
	if event.IsTimeoutKind() {
		return
	}

	if notifyInput {
		p.input.Commit(event)
		p.outputEvents.Inc()
		p.outputSize.Add(int64(event.Size))

		if len(p.outSample) == 0 && rand.Int()&1 == 1 {
			p.outSample = event.Root.Encode(p.outSample)
		}

		if event.Size > p.maxSize {
			p.maxSize = event.Size
		}
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

	p.eventPool.back(event)
}

func (p *Pipeline) AddAction(info *ActionPluginStaticInfo) {
	p.actionInfos = append(p.actionInfos, info)
	p.metricsHolder.AddAction(info.MetricName, info.MetricLabels)
}

func (p *Pipeline) initProcs() {
	// default proc count is CPU cores * 2
	procCount := runtime.GOMAXPROCS(0)
	if p.singleProc {
		procCount = 1
	}
	p.logger.Infof("starting pipeline %q: procs=%d", p.Name, procCount)

	p.procCount = atomic.NewInt32(int32(procCount))
	p.activeProcs = atomic.NewInt32(0)

	p.Procs = make([]*processor, 0, procCount)
	for i := 0; i < procCount; i++ {
		p.Procs = append(p.Procs, p.newProc())
	}
}

func (p *Pipeline) newProc() *processor {
	proc := NewProcessor(
		p.metricsHolder,
		p.activeProcs,
		p.output,
		p.streamer,
		p.finalize,
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
		if p.shouldStop {
			return
		}
		if p.procCount.Load() != p.activeProcs.Load() {
			t = time.Now()
		}

		if time.Since(t) > interval {
			p.expandProcs()
		}
	}
}

func (p *Pipeline) expandProcs() {
	if p.singleProc {
		return
	}

	from := p.procCount.Load()
	to := from * 2
	p.logger.Infof("processors count expanded from %d to %d", from, to)
	if to > 10000 {
		p.logger.Warnf("too many processors: %d", to)
	}

	for x := 0; x < int(to-from); x++ {
		proc := p.newProc()
		p.Procs = append(p.Procs, proc)
		proc.registerMetrics(p.metricsCtl)
		proc.start(p.actionParams, p.logger)
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
	inputSize := p.inputSize.Load()
	inputEvents := p.inputEvents.Load()
	inUseEvents := p.eventPool.inUseEvents.Load()

	interval := p.settings.MaintenanceInterval
	rate := int(myDeltas.deltaInputEvents * float64(time.Second) / float64(interval))
	rateMb := myDeltas.deltaInputSize * float64(time.Second) / float64(interval) / 1024 / 1024
	readOps := int(myDeltas.deltaReads * float64(time.Second) / float64(interval))
	tc := int64(math.Max(float64(inputSize), 1))

	p.logger.Infof(`%q pipeline stats interval=%ds, active procs=%d/%d, events outside pool=%d/%d, events in pool=%d/%d, out=%d|%.1fMb,`+
		`rate=%d/s|%.1fMb/s, read ops=%d/s, total=%d|%.1fMb, avg size=%d, max size=%d`,
		p.Name, interval/time.Second, p.activeProcs.Load(), p.procCount.Load(),
		inUseEvents, p.settings.Capacity, p.settings.Capacity-int(inUseEvents), p.settings.Capacity,
		int64(myDeltas.deltaInputEvents), float64(myDeltas.deltaInputSize)/1024.0/1024.0, rate, rateMb, readOps,
		inputEvents, float64(inputSize)/1024.0/1024.0, inputSize/tc, p.maxSize)
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

	p.inputEventsCountMetric.WithLabelValues().Add(myDeltas.deltaInputEvents)
	p.inputEventSizeMetric.WithLabelValues().Add(myDeltas.deltaInputSize)
	p.outputEventsCountMetric.WithLabelValues().Add(myDeltas.deltaOutputEvents)
	p.outputEventSizeMetric.WithLabelValues().Add(myDeltas.deltaOutputSize)
	p.readOpsEventsSizeMetric.WithLabelValues().Add(myDeltas.deltaReads)

	return myDeltas
}

func (p *Pipeline) setMetrics(inUseEvents atomic.Int64) {
	p.inUseEventsMetric.WithLabelValues().Set(float64(inUseEvents.Load()))
}

func (p *Pipeline) maintenance() {
	inputEvents := newDeltaWrapper()
	inputSize := newDeltaWrapper()
	outputEvents := newDeltaWrapper()
	outputSize := newDeltaWrapper()
	readOps := newDeltaWrapper()

	for {
		time.Sleep(p.settings.MaintenanceInterval)
		if p.shouldStop {
			return
		}

		p.antispamer.maintenance()
		p.metricsHolder.maintenance()

		myDeltas := p.incMetrics(inputEvents, inputSize, outputEvents, outputSize, readOps)
		p.setMetrics(p.eventPool.inUseEvents)
		p.logChanges(myDeltas)

		if len(p.inSample) > 0 {
			p.logger.Infof("%q pipeline input event sample: %s", p.Name, p.inSample)
			p.inSample = p.inSample[:0]
		}

		if len(p.outSample) > 0 {
			p.logger.Infof("%q pipeline output event sample: %s", p.Name, p.outSample)
			p.outSample = p.outSample[:0]
		}
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

func (p *Pipeline) SuggestDecoder(t decoder.DecoderType) {
	p.suggestedDecoder = t
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
	if index >= len(p.eventLog) {
		p.logger.Fatalf("can't find log item with index %d", index)
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
	_, _ = w.Write([]byte(p.antispamer.dump()))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}

// serveActionInfo creates a handlerFunc for the given action.
// it returns metric values for the given action.
func (p *Pipeline) serveActionInfo(info ActionPluginStaticInfo) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Add("Content-Type", "application/json")

		type Event struct {
			Status string `json:"status"`
			Count  int    `json:"count"`
		}

		if info.MetricName == "" {
			writeErr(w, "If you want to see a statistic about events, consider adding `metric_name` to the action's configuration.")
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		var actionMetric *metrics
		for _, m := range p.metricsHolder.metrics {
			if m.name == info.MetricName {
				actionMetric = m

				break
			}
		}

		events := []Event{}
		for _, status := range []eventStatus{
			eventStatusReceived,
			eventStatusDiscarded,
			eventStatusPassed,
		} {
			c := actionMetric.current.totalCounter[string(status)]
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
			writeErr(w, "There are no active processors")
			w.WriteHeader(http.StatusBadRequest)

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
			writeErr(w, "Timeout while try to display an event before and after the action processing.")
			w.WriteHeader(http.StatusInternalServerError)
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
