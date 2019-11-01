package pipeline

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const (
	defaultFieldValue   = "not_set"
	maintenanceInterval = time.Second * 5
	metricsGenInterval  = time.Minute * 10
)

type InputPluginController interface {
	In(sourceId SourceID, sourceName string, offset int64, bytes []byte)
	Reset(sourceId SourceID) int // mark source events in the pipeline as isDeprecated, it means that these events shouldn't update offsets
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
	Name      string
	settings  *Settings
	eventPool *eventPool

	input      InputPlugin
	inputData  *InputPluginData
	Processors []*processor
	output     OutputPlugin
	outputData *OutputPluginData

	streamNames []string
	streamer    *streamer

	doneWg *sync.WaitGroup

	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex

	stopCh chan bool

	// metrics
	registry       *prometheus.Registry
	metricsGen     int // generation is used to drop unused metrics from counters
	metricsGenTime time.Time
	actionMetrics  []*actionMetrics

	// some debugging shit
	eventSample    []byte
	totalCommitted atomic.Int64
	totalIn        atomic.Int64
}

type Settings struct {
	Capacity             int
	AvgLogSize           int
	ProcessorsCount      int
	StreamField          string
	isStreamFieldEnabled bool
}

type actionMetrics struct {
	name   string
	labels []string

	counter     *prometheus.CounterVec
	counterPrev *prometheus.CounterVec

	mu *sync.RWMutex
}

func New(name string, settings *Settings, registry *prometheus.Registry) *Pipeline {
	logger.Infof("creating pipeline %q: processors=%d, capacity=%d, stream field=%s", name, settings.ProcessorsCount, settings.Capacity, settings.StreamField)
	pipeline := &Pipeline{
		Name:     name,
		settings: settings,

		streamer: newStreamer(),

		doneWg: &sync.WaitGroup{},

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop

		registry:      registry,
		actionMetrics: make([]*actionMetrics, 0, 0),
	}

	settings.isStreamFieldEnabled = settings.StreamField != "off"

	processors := make([]*processor, 0, 0)
	for i := 0; i < settings.ProcessorsCount; i++ {
		processors = append(processors, NewProcessor(i, pipeline, pipeline.streamer))
	}
	pipeline.Processors = processors

	pipeline.eventPool = newEventPool(pipeline.settings.Capacity)

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
	actionParams := &ActionPluginParams{
		PluginDefaultParams: defaultParams,
		Controller:          p,
	}
	outputParams := &OutputPluginParams{
		PluginDefaultParams: defaultParams,
		Controller:          p,
	}

	p.output.Start(p.outputData.Config, outputParams)

	rnd := make([]byte, 0, 0)
	for _, processor := range p.Processors {
		processor.start(p.output, actionParams)
		if !p.settings.isStreamFieldEnabled {
			rnd = append(rnd, byte('a'+rand.Int()%('z'-'a')))
			crc := crc32.ChecksumIEEE(rnd)
			p.streamNames = append(p.streamNames, strconv.FormatUint(uint64(crc), 8))
		}
	}

	p.input.Start(p.inputData.Config, inputParams)

	p.streamer.start()

	go p.maintenance()
}

func (p *Pipeline) Stop() {
	logger.Infof("stopping pipeline %q, total: in=%d, processed=%d", p.Name, p.totalIn.Load(), p.totalCommitted.Load())
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

func (p *Pipeline) Reset(sourceId SourceID) int {
	count := 0
	p.eventPool.visit(func(e *Event) {
		if e.SourceID != sourceId {
			return
		}

		e.deprecate()
		count++
	})

	return count
}

func (p *Pipeline) In(sourceID SourceID, sourceName string, offset int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	x := p.totalIn.Inc()

	size := len(bytes)
	event, err := p.eventPool.get(bytes)
	if err != nil {
		logger.Fatalf("wrong json offset=%d, length=%d, err=%s, source=%d:%s, json=%s", offset, size, err.Error(), sourceID, sourceName, bytes)
		return
	}

	stream := StreamName("not_set")
	if p.settings.isStreamFieldEnabled {
		streamNode := event.Root.Dig("stream")
		if streamNode != nil {
			stream = StreamName(streamNode.AsBytes()) // as bytes because we need a copy
		}
	}

	event.Offset = offset
	event.SourceID = sourceID
	event.SourceName = sourceName
	event.StreamName = stream
	event.Size = len(bytes)

	if !p.settings.isStreamFieldEnabled {
		index := int(x) % len(p.streamNames)
		stream = StreamName(p.streamNames[index])
		sourceID = SourceID(index)
	}

	//logger.Infof("commit: %d %s", event.Offset, event.Root.EncodeToString())
	p.streamer.putEvent(event)
}

func (p *Pipeline) Commit(event *Event) {
	p.commit(event, true)
}

func (p *Pipeline) Propagate(event *Event) {
	p.commit(event, true)
}

func (p *Pipeline) commit(event *Event, notifyInput bool) {
	event.stage = eventStageBack
	if notifyInput {
		if len(p.eventSample) == 0 {
			p.eventSample = event.Root.Encode(p.eventSample)
		}
		p.input.Commit(event)
	}

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, event.Root.EncodeToString())
		p.eventLogMu.Unlock()
	}

	event.stream.commit(event)
	p.eventPool.back(event)
	p.totalCommitted.Inc()
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

		value := defaultFieldValue
		if node != nil {
			value = node.AsString()
		}

		valuesBuf = append(valuesBuf, value)
	}

	metrics.mu.RLock()
	metrics.counter.WithLabelValues(valuesBuf...).Inc()
	metrics.mu.RUnlock()

	return valuesBuf
}

func (p *Pipeline) maintenance() {
	lastProcessed := int64(0)
	time.Sleep(maintenanceInterval)
	for {
		select {
		case <-p.stopCh:
			return
		default:
			logger.Infof("pipeline %q final event sample: %s", p.Name, p.eventSample)
			p.eventSample = p.eventSample[:0]

			//if p.eventPool.eventsCount == p.settings.Capacity {
			//	p.streamsMu.Lock()
			//	logger.Infof("========streams======== ready=%d", len(p.readyStreams))
			//	for _, s := range p.streams {
			//		for _, stream := range s {
			//			if stream.isDetaching {
			//				logger.Infof("stream %d(%s) state=DETACHING, get offset=%d, commit offset=%d", stream.sourceId, stream.name, stream.getOffset, stream.commitOffset)
			//			}
			//			if stream.isAttached {
			//				logger.Infof("stream %d(%s) state=ATTACHED", stream.sourceId, stream.name)
			//			}
			//		}
			//	}
			//	p.streamsMu.Unlock()
			//	logger.Infof("========processor state dump========")
			//	for _, t := range p.Processors {
			//		t.dumpState()
			//	}
			//
			//	logger.Infof("========default events dump========")
			//	p.eventPool.visit(func(event *Event) {
			//		logger.Infof("event: index=%d, id=%d, stream=%d(%s), stage=%s", event.index, event.SeqID, event.SourceID, event.StreamName, event.stageStr())
			//	})
			//}

			processed := p.totalCommitted.Load()
			delta := processed - lastProcessed
			lastProcessed = processed
			rate := delta * int64(time.Second) / int64(maintenanceInterval)
			logger.Infof("pipeline %q stats for last %d seconds: processed=%d, rate=%d/sec, queue=%d/%d, total processed=%d, max log size=%d", p.Name, maintenanceInterval/time.Second, delta, rate, p.eventPool.eventsCount, p.settings.Capacity, processed, p.eventPool.maxEventSize)

			if time.Now().Sub(p.metricsGenTime) > metricsGenInterval {
				p.nextMetricsGen()
			}

			time.Sleep(maintenanceInterval)
		}
	}
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
		//
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
