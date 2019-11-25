package pipeline

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const (
	DefaultStreamField         = "stream"
	DefaultCapacity            = 1024
	DefaultAvgLogSize          = 32 * 1024
	DefaultNodePoolSize        = 1024
	DefaultMaintenanceInterval = time.Second * 5
	defaultFieldValue          = "not_set"
	defaultStreamName          = StreamName("not_set")

	unbanIterations    = 4
	metricsGenInterval = time.Minute * 10
)

type InputPluginController interface {
	In(sourceID SourceID, sourceName string, offset int64, bytes []byte)
	DeprecateSource(sourceID SourceID) int // mark events in the pipeline as deprecated, it means that these events shouldn't update offsets on commit
	DisableStreams()                       // don't use stream field and spread all events across all processors
}

type ActionPluginController interface {
	Commit(event *Event)    // commit offset of held event and skip further processing
	Propagate(event *Event) // throw held event back to pipeline
}
type OutputPluginController interface {
	Commit(event *Event) // notify input plugin that event is successfully processed and save offsets
}

type SourceID uint64
type StreamName string

type Pipeline struct {
	Name       string
	settings   *Settings
	eventPool  *eventPool
	useStreams bool
	streamer   *streamer
	doneWg     *sync.WaitGroup
	stopCh     chan bool
	countersMu *sync.RWMutex
	counters   map[SourceID]*atomic.Int32

	input      InputPlugin
	inputData  *InputPluginData
	Processors []*processor
	output     OutputPlugin
	outputData *OutputPluginData

	// metrics
	registry       *prometheus.Registry
	metricsGen     int // generation is used to drop unused metrics from counters
	metricsGenTime time.Time
	actionMetrics  []*actionMetrics

	// some debugging shit
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
	Capacity            int
	MaintenanceInterval time.Duration
	AntispamThreshold   int
	AvgLogSize          int
	ProcessorsCount     int
	StreamField         string
}

type actionMetrics struct {
	name   string
	labels []string

	counter     *prometheus.CounterVec
	counterPrev *prometheus.CounterVec

	mu *sync.RWMutex
}

func New(name string, settings *Settings, registry *prometheus.Registry, mux *http.ServeMux) *Pipeline {
	logger.Infof("creating pipeline %q: processors=%d, capacity=%d, stream field=%s", name, settings.ProcessorsCount, settings.Capacity, settings.StreamField)
	pipeline := &Pipeline{
		Name:       name,
		settings:   settings,
		useStreams: true,
		stopCh:     make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
		doneWg:     &sync.WaitGroup{},
		counters:   make(map[SourceID]*atomic.Int32),
		countersMu: &sync.RWMutex{},

		streamer:      newStreamer(),
		registry:      registry,
		actionMetrics: make([]*actionMetrics, 0, 0),

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},
	}

	if settings.AntispamThreshold != 0 {
		logger.Infof("antispam: enabled threshold=%d/s", settings.AntispamThreshold/int(settings.MaintenanceInterval/time.Second))
	}

	processors := make([]*processor, 0, 0)
	for i := 0; i < settings.ProcessorsCount; i++ {
		processors = append(processors, NewProcessor(i, pipeline, pipeline.streamer))
	}
	pipeline.Processors = processors

	pipeline.eventPool = newEventPool(pipeline.settings.Capacity)

	mux.HandleFunc("/pipelines/"+name, pipeline.servePipeline)

	return pipeline
}

func (p *Pipeline) Start() {
	if p.input == nil {
		logger.Panicf("input isn't set for pipeline %q", p.Name)
	}
	if p.output == nil {
		logger.Panicf("output isn't set for pipeline %q", p.Name)
	}

	p.nextMetricsGen()
	p.HandleEventFlowStart()

	defaultParams := &PluginDefaultParams{
		PipelineName:     p.Name,
		PipelineSettings: p.settings,
	}
	inputParams := &InputPluginParams{
		PluginDefaultParams: defaultParams,
		Controller:          p,
		DoneWg:              p.doneWg,
	}
	outputParams := &OutputPluginParams{
		PluginDefaultParams: defaultParams,
		Controller:          p,
	}

	p.output.Start(p.outputData.Config, outputParams)

	for _, processor := range p.Processors {
		actionParams := &ActionPluginParams{
			PluginDefaultParams: defaultParams,
			Controller:          processor,
		}
		processor.start(p.output, actionParams)
	}

	p.input.Start(p.inputData.Config, inputParams)

	p.streamer.start()

	go p.maintenance()
}

func (p *Pipeline) Stop() {
	logger.Infof("stopping pipeline %q", p.Name)
	p.stopCh <- true

	for _, processor := range p.Processors {
		processor.stop()
	}

	logger.Infof("stopping %q input", p.Name)
	p.input.Stop()

	logger.Infof("stopping %q output", p.Name)
	p.output.Stop()
}

func (p *Pipeline) SetInputPlugin(descr *InputPluginData) {
	p.inputData = descr
	p.input = descr.Plugin
}

func (p *Pipeline) SetOutputPlugin(descr *OutputPluginData) {
	p.outputData = descr
	p.output = descr.Plugin
}

func (p *Pipeline) DeprecateSource(sourceID SourceID) int {
	count := 0
	p.eventPool.visit(func(e *Event) {
		if e.SourceID != sourceID {
			return
		}

		e.SetIgnoreKind()
		count++
	})

	return count
}

func (p *Pipeline) In(sourceID SourceID, sourceName string, offset int64, bytes []byte) {
	length := len(bytes)
	if length == 0 || p.isBanned(sourceID, sourceName, (offset-int64(length)) <= 1) {
		return
	}

	event := p.eventPool.get()
	err := event.parseJSON(bytes)
	if err != nil {
		logger.Fatalf("wrong json offset=%d, length=%d, err=%s, source=%d:%s, json=%s", offset, length, err.Error(), sourceID, sourceName, bytes)
		return
	}

	event.Offset = offset
	event.SourceID = sourceID
	event.SourceName = sourceName
	event.streamName = defaultStreamName
	event.Size = len(bytes)

	if len(p.inSample) == 0 && rand.Int()&1 == 1 {
		p.inSample = event.Root.Encode(p.inSample)
	}

	p.streamEvent(event)
}

func (p *Pipeline) isBanned(id SourceID, name string, isNew bool) bool {
	if p.settings.AntispamThreshold == 0 {
		return false
	}

	p.countersMu.RLock()
	value, has := p.counters[id]
	p.countersMu.RUnlock()

	if !has {
		p.countersMu.Lock()
		value = &atomic.Int32{}
		p.counters[id] = value
		p.countersMu.Unlock()
	}

	if isNew {
		logger.Infof("antispam: new source added %d:%s", id, name)
		value.Swap(0)
	}

	x := value.Inc()
	if x == int32(p.settings.AntispamThreshold) {
		value.Swap(int32(unbanIterations * p.settings.AntispamThreshold))
		logger.Warnf("antispam: source %d:%s has been banned pipeline=%q, threshold=%d/s", id, name, p.Name, p.settings.AntispamThreshold/int(p.settings.MaintenanceInterval/time.Second))
	}

	return x >= int32(p.settings.AntispamThreshold)
}

func (p *Pipeline) streamEvent(event *Event) {
	// spread events across all processors
	if !p.useStreams {
		sourceID := SourceID(event.SeqID % uint64(p.settings.ProcessorsCount))
		p.streamer.putEvent(sourceID, defaultStreamName, event)
		return
	}

	node := event.Root.Dig(p.settings.StreamField)
	if node != nil {
		event.streamName = StreamName(node.AsString())
	}
	p.streamer.putEvent(event.SourceID, event.streamName, event)
}

func (p *Pipeline) DisableStreams() {
	p.useStreams = false
}

func (p *Pipeline) Commit(event *Event) {
	p.finalize(event, true, true)
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

func (p *Pipeline) AddAction(info *PluginInfo, configJSON []byte, matchMode MatchMode, conditions MatchConditions, metricName string, metricLabels []string) {
	p.actionMetrics = append(p.actionMetrics, &actionMetrics{
		name:        metricName,
		labels:      metricLabels,
		counter:     nil,
		counterPrev: nil,
		mu:          &sync.RWMutex{},
	})

	for index, processor := range p.Processors {
		plugin, config := info.Factory()
		err := json.Unmarshal(configJSON, config)
		if err != nil {
			logger.Panicf("can't unmarshal config for action #%d in pipeline %q: %s", index, p.Name, err.Error())
		}

		processor.AddActionPlugin(&ActionPluginData{
			Plugin: plugin.(ActionPlugin),
			PluginDesc: PluginDesc{
				ID:     strconv.Itoa(index) + "_" + strconv.Itoa(index),
				T:      info.Type,
				Config: config,
			},
			MatchConditions: conditions,
			MatchMode:       matchMode,
		})
	}
}

func (p *Pipeline) nextMetricsGen() {
	metricsGen := strconv.Itoa(p.metricsGen)

	for index, metrics := range p.actionMetrics {
		if metrics.name == "" {
			continue
		}

		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "filed",
			Subsystem:   "pipeline_" + p.Name,
			Name:        metrics.name + "_events_total",
			Help:        fmt.Sprintf("how many events processed by pipeline %q and #%d action", p.Name, index),
			ConstLabels: map[string]string{"gen": metricsGen},
		}, append([]string{"status"}, metrics.labels...))

		prev := metrics.counterPrev

		metrics.mu.Lock()
		metrics.counterPrev = metrics.counter
		metrics.counter = counter
		metrics.mu.Unlock()

		p.registry.MustRegister(counter)
		if prev != nil {
			p.registry.Unregister(prev)
		}
	}

	p.metricsGen++
	p.metricsGenTime = time.Now()
}

func (p *Pipeline) countEvent(event *Event, actionIndex int, eventStatus eventStatus, valuesBuf []string) []string {
	if len(p.actionMetrics) == 0 {
		return valuesBuf
	}

	metrics := p.actionMetrics[actionIndex]
	if metrics.name == "" {
		return valuesBuf
	}

	valuesBuf = valuesBuf[:0]
	valuesBuf = append(valuesBuf, string(eventStatus))

	for _, field := range metrics.labels {
		node := event.Root.Dig(field)

		if node == nil {
			valuesBuf = append(valuesBuf, defaultFieldValue)
		} else {
			valuesBuf = append(valuesBuf, node.AsString())
		}
	}

	metrics.mu.RLock()
	metrics.counter.WithLabelValues(valuesBuf...).Inc()
	metrics.mu.RUnlock()

	return valuesBuf
}

func (p *Pipeline) maintenance() {
	lastCommitted := int64(0)
	lastSize := int64(0)
	interval := p.settings.MaintenanceInterval
	for {
		time.Sleep(interval)
		select {
		case <-p.stopCh:
			return
		default:
			p.maintenanceCounters()
			p.maintenanceMetrics()

			totalCommitted := p.totalCommitted.Load()
			deltaCommitted := int(totalCommitted - lastCommitted)

			totalSize := p.totalSize.Load()
			deltaSize := int(totalSize - lastSize)

			rate := int(float64(deltaCommitted) * float64(time.Second) / float64(interval))
			rateMb := float64(deltaSize) * float64(time.Second) / float64(interval) / 1024 / 1024

			if totalCommitted == 0 {
				totalCommitted = 1
			}

			logger.Infof("%q pipeline stats interval=%ds, queue=%d/%d, out=%d|%.1fMb, rate=%d/s|%.1fMb/s, total=%d|%.1fMb, avg size=%d, max size=%d", p.Name, interval/time.Second, p.eventPool.eventsCount, p.settings.Capacity, deltaCommitted, float64(deltaSize)/1024.0/1024.0, rate, rateMb, totalCommitted, float64(totalSize)/1024.0/1024.0, totalSize/totalCommitted, p.maxSize)

			lastCommitted = totalCommitted
			lastSize = totalSize

			if len(p.inSample) > 0 {
				logger.Infof("%q pipeline input event sample: %s", p.Name, p.inSample)
				p.inSample = p.inSample[:0]
			}

			if len(p.outSample) > 0 {
				logger.Infof("%q pipeline output event sample: %s", p.Name, p.outSample)
				p.outSample = p.outSample[:0]
			}
		}
	}
}

func (p *Pipeline) maintenanceMetrics() {
	if time.Now().Sub(p.metricsGenTime) < metricsGenInterval {
		return
	}

	p.nextMetricsGen()
}

func (p *Pipeline) maintenanceCounters() {
	p.countersMu.Lock()
	for source, counter := range p.counters {
		x := int(counter.Load())

		if x == 0 {
			delete(p.counters, source)
			continue
		}

		isMore := x >= p.settings.AntispamThreshold
		x -= p.settings.AntispamThreshold
		if x < 0 {
			x = 0
		}

		if isMore && x < p.settings.AntispamThreshold {
			logger.Infof("antispam: source %d has been unbanned pipeline=%q, threshold=%d/s", source, p.Name, p.settings.AntispamThreshold/int(p.settings.MaintenanceInterval/time.Second))
		}

		if x > unbanIterations*p.settings.AntispamThreshold {
			x = unbanIterations * p.settings.AntispamThreshold
		}

		counter.Swap(int32(x))
	}
	p.countersMu.Unlock()
}

func (p *Pipeline) HandleEventFlowStart() {
	p.doneWg.Add(1)
}

func (p *Pipeline) HandleEventFlowFinish(instant bool) {
	if instant {
		p.doneWg.Done()
		return
	}

	// fs events may have delay, so wait for them
	time.Sleep(time.Millisecond * 100)
	p.doneWg.Done()
}

func (p *Pipeline) WaitUntilDone(instant bool) {
	if instant {
		p.doneWg.Wait()
		return
	}

	for {
		p.doneWg.Wait()

		processed := p.totalCommitted.Load()
		//fs events may have delay, so wait for them
		time.Sleep(time.Millisecond * 100)

		processed = p.totalCommitted.Load() - processed
		if processed == 0 {
			break
		}
	}
}

func (p *Pipeline) GetEventsTotal() int {
	return int(p.totalCommitted.Load())
}

func (p *Pipeline) EnableEventLog() {
	p.eventLogEnabled = true
}

func (p *Pipeline) GetEventLogLength() int {
	return len(p.eventLog)
}

func (p *Pipeline) GetEventLogItem(index int) string {
	if index >= len(p.eventLog) {
		logger.Fatalf("can't find log item with index %d", index)
	}
	return p.eventLog[index]
}

func (p *Pipeline) servePipeline(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("<html><body><pre><p>"))
	_, _ = w.Write([]byte(logger.Header("pipeline " + p.Name)))
	_, _ = w.Write([]byte(p.streamer.dump()))
	_, _ = w.Write([]byte(p.eventPool.dump()))

	_, _ = w.Write([]byte("</p></pre></body></html>"))
}
