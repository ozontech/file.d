package pipeline

import (
	"math/rand"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ozonru/file.d/decoder"
	"github.com/ozonru/file.d/logger"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	DefaultStreamField         = "stream"
	DefaultCapacity            = 1024
	DefaultAvgLogSize          = 16 * 1024
	DefaultJSONNodePoolSize    = 1024
	DefaultMaintenanceInterval = time.Second * 5
	DefaultFieldValue          = "not_set"
	DefaultStreamName          = StreamName("not_set")

	antispamUnbanIterations = 4
	metricsGenInterval      = time.Hour
)

type finalizeFn = func(event *Event, notifyInput bool, backEvent bool)

type InputPluginController interface {
	In(sourceID SourceID, sourceName string, offset int64, data []byte, isNewSource bool) uint64
	DisableStreams()                      // don't use stream field and spread all events across all processors
	SuggestDecoder(t decoder.DecoderType) // set decoder if pipeline uses "auto" value for decoder
}

type ActionPluginController interface {
	Commit(event *Event)    // commit offset of held event and skip further processing
	Propagate(event *Event) // throw held event back to pipeline
}

type OutputPluginController interface {
	Commit(event *Event) // notify input plugin that event is successfully processed and save offsets
	Error(err string)
}

type SourceID uint64
type StreamName string

type Pipeline struct {
	Name     string
	settings *Settings

	decoder          decoder.DecoderType // decoder set in the config
	suggestedDecoder decoder.DecoderType // decoder suggested by input plugin, it is used when config decoder is set to "auto"

	eventPool *eventPool
	streamer  *streamer

	useStreams bool
	singleProc bool
	shouldStop bool

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

	// some debugging shit
	logger          *zap.SugaredLogger
	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex
	inSample        []byte
	outSample       []byte
	totalCommitted  atomic.Int64
	totalSize       atomic.Int64
	maxSize         int
}

type Settings struct {
	Decoder             string
	Capacity            int
	MaintenanceInterval time.Duration
	AntispamThreshold   int
	AvgLogSize          int
	StreamField         string
	IsStrict            bool
}

func New(name string, settings *Settings, registry *prometheus.Registry, mux *http.ServeMux) *Pipeline {
	pipeline := &Pipeline{
		Name:       name,
		logger:     logger.Instance.Named(name),
		settings:   settings,
		useStreams: true,
		actionParams: &PluginDefaultParams{
			PipelineName:     name,
			PipelineSettings: settings,
		},

		metricsHolder: newMetricsHolder(name, registry, metricsGenInterval),
		streamer:      newStreamer(),
		eventPool:     newEventPool(settings.Capacity),
		antispamer:    newAntispamer(settings.AntispamThreshold, antispamUnbanIterations, settings.MaintenanceInterval),

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},
	}

	switch settings.Decoder {
	case "json":
		pipeline.decoder = decoder.JSON
	case "raw":
		pipeline.decoder = decoder.RAW
	case "cri":
		pipeline.decoder = decoder.CRI
	case "postgres":
		pipeline.decoder = decoder.POSTGRES
	case "auto":
		pipeline.decoder = decoder.AUTO
	default:
		pipeline.logger.Fatalf("unknown decoder %q for pipeline %q", settings.Decoder, name)
	}

	mux.HandleFunc("/pipelines/"+name, pipeline.servePipeline)

	return pipeline
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
	p.output.Start(p.outputInfo.Config, outputParams)

	p.logger.Infof("stating processors, count=%d", len(p.Procs))
	for _, processor := range p.Procs {
		processor.start(p.actionParams, p.logger)
	}

	p.logger.Infof("starting input plugin %q", p.inputInfo.Type)
	inputParams := &InputPluginParams{
		PluginDefaultParams: p.actionParams,
		Controller:          p,
		Logger:              p.logger.Named("input " + p.inputInfo.Type),
	}
	p.input.Start(p.inputInfo.Config, inputParams)

	p.streamer.start()

	go p.maintenance()
	go p.growProcs()
}

func (p *Pipeline) Stop() {
	p.logger.Infof("stopping pipeline %q, total committed=%d", p.Name, p.totalCommitted.Load())

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

func (p *Pipeline) In(sourceID SourceID, sourceName string, offset int64, bytes []byte, isNewSource bool) uint64 {
	length := len(bytes)

	// don't process shit
	isEmpty := length == 0 || (bytes[0] == '\n' && length == 1)
	isSpam := p.antispamer.isSpam(sourceID, sourceName, isNewSource)
	if isEmpty || isSpam {
		return 0
	}

	event := p.eventPool.get()

	dec := decoder.NO
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
			p.logger.Fatalf("wrong json format offset=%d, length=%d, err=%s, source=%d:%s, json=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			return 0
		}
	case decoder.RAW:
		_ = event.Root.DecodeString("{}")
		event.Root.AddFieldNoAlloc(event.Root, "message").MutateToBytesCopy(event.Root, bytes[:len(bytes)-1])
	case decoder.CRI:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodeCRI(event.Root, bytes)
		if err != nil {
			p.logger.Fatalf("wrong cri format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			return 0
		}
	case decoder.POSTGRES:
		_ = event.Root.DecodeString("{}")
		err := decoder.DecodePostgres(event.Root, bytes)
		if err != nil {
			p.logger.Fatalf("wrong postgres format offset=%d, length=%d, err=%s, source=%d:%s, cri=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
			return 0
		}
	default:
		p.logger.Panicf("unknown decoder %d for pipeline %q", p.decoder, p.Name)
	}

	event.Offset = offset
	event.SourceID = sourceID
	event.SourceName = sourceName
	event.streamName = DefaultStreamName
	event.Size = len(bytes)

	if len(p.inSample) == 0 {
		p.inSample = event.Root.Encode(p.inSample)
	}

	return p.streamEvent(event)
}

func (p *Pipeline) streamEvent(event *Event) uint64 {
	// spread events across all processors
	if !p.useStreams {
		sourceID := SourceID(event.SeqID % uint64(p.procCount.Load()))

		return p.streamer.putEvent(sourceID, DefaultStreamName, event)
	}

	node := event.Root.Dig(p.settings.StreamField)
	if node != nil {
		event.streamName = StreamName(node.AsString())
	}

	return p.streamer.putEvent(event.SourceID, event.streamName, event)
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

		p.totalCommitted.Inc()
		p.totalSize.Add(int64(event.Size))

		if len(p.outSample) == 0 && rand.Int()&1 == 1 {
			p.outSample = event.Root.Encode(p.outSample)
		}

		if event.Size > p.maxSize {
			p.maxSize = event.Size
		}
	}

	// todo: avoid shitty event.stream.commit(event)
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
	procCount := runtime.GOMAXPROCS(0) * 2
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
	proc := NewProcessor(p.metricsHolder, p.activeProcs, p.output, p.streamer, p.finalize)
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

		if time.Now().Sub(t) > interval {
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
		proc.start(p.actionParams, p.logger)
	}

	p.procCount.Swap(to)
}

func (p *Pipeline) maintenance() {
	lastCommitted := int64(0)
	lastSize := int64(0)
	interval := p.settings.MaintenanceInterval
	for {
		time.Sleep(interval)
		if p.shouldStop {
			return
		}

		p.antispamer.maintenance()
		p.metricsHolder.maintenance()

		totalCommitted := p.totalCommitted.Load()
		deltaCommitted := int(totalCommitted - lastCommitted)

		totalSize := p.totalSize.Load()
		deltaSize := int(totalSize - lastSize)

		rate := int(float64(deltaCommitted) * float64(time.Second) / float64(interval))
		rateMb := float64(deltaSize) * float64(time.Second) / float64(interval) / 1024 / 1024

		tc := totalCommitted
		if totalCommitted == 0 {
			tc = 1
		}

		p.logger.Infof("%q pipeline stats interval=%ds, active procs=%d/%d, queue=%d/%d, out=%d|%.1fMb, rate=%d/s|%.1fMb/s, total=%d|%.1fMb, avg size=%d, max size=%d", p.Name, interval/time.Second, p.activeProcs.Load(), p.procCount.Load(), p.settings.Capacity-p.eventPool.freeEventsCount, p.settings.Capacity, deltaCommitted, float64(deltaSize)/1024.0/1024.0, rate, rateMb, totalCommitted, float64(totalSize)/1024.0/1024.0, totalSize/tc, p.maxSize)

		lastCommitted = totalCommitted
		lastSize = totalSize

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

func (p *Pipeline) DisableStreams() {
	p.useStreams = false
}

func (p *Pipeline) SuggestDecoder(t decoder.DecoderType) {
	p.suggestedDecoder = t
}

func (p *Pipeline) DisableParallelism() {
	p.singleProc = true
}

func (p *Pipeline) GetEventsTotal() int {
	return int(p.totalCommitted.Load())
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
