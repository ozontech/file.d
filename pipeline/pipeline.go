package pipeline

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const (
	defaultCapacity     = 2048
	defaultFieldValue   = "not_set"
	maintenanceInterval = time.Second * 5
	metricsGenInterval  = time.Minute * 10
)

const (
	eventsPoolDefault = 0
	eventsPoolVIP     = 1
)

type Head interface {
	In(sourceId SourceId, sourceName string, offset int64, size int64, bytes []byte)
	Deprecate(sourceId SourceId) // mark source events in the pipeline as deprecated, it means that these events shouldn't update offsets
}

type Tail interface {
	Commit(event *Event) // notify input plugin that event is successfully processed and save offsets
}

type SourceId uint64
type StreamName string

type Pipeline struct {
	name          string
	capacity      int
	eventsDefault *eventPool // common events pool
	eventsVIP     *eventPool // for events which needed by processors waiting for events from specific stream

	input      InputPlugin
	inputData  *InputPluginData
	Processors []*processor
	output     OutputPlugin
	outputData *OutputPluginData

	waitingProcessors []atomic.Uint64

	streams   map[SourceId]map[StreamName]*stream
	streamsMu *sync.RWMutex

	doneWg *sync.WaitGroup

	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex

	stopCh chan bool

	// metrics
	registry       *prometheus.Registry
	metricsGen     int // generation is used to drop unused metrics from counters
	metricsGenTime time.Time

	// some debugging shit
	totalCommitted atomic.Int64
	totalIn        atomic.Int64
	sampleEvery    uint64
}

func New(name string, processorsCount int, sampleEvery uint64, registry *prometheus.Registry) *Pipeline {
	capacity := defaultCapacity
	logger.Infof("creating new pipeline with processors=%d capacity=%d", processorsCount, capacity)
	pipeline := &Pipeline{
		name:     name,
		capacity: capacity,

		waitingProcessors: make([]atomic.Uint64, processorsCount),

		streams:   make(map[SourceId]map[StreamName]*stream),
		streamsMu: &sync.RWMutex{},

		doneWg: &sync.WaitGroup{},

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop

		registry:    registry,
		sampleEvery: sampleEvery,
	}

	processors := make([]*processor, processorsCount)
	for i := 0; i < processorsCount; i++ {
		processors[i] = NewProcessor(i, pipeline)
	}
	pipeline.Processors = processors

	pipeline.eventsDefault = newEventPool(eventsPoolDefault, pipeline.capacity)
	pipeline.eventsVIP = newEventPool(eventsPoolVIP, pipeline.capacity)

	return pipeline
}

func (p *Pipeline) Start() {
	p.nextMetricsGeneration()
	p.HandleEventFlowStart()

	if p.input == nil {
		logger.Panicf("input isn't set for pipeline %q", p.name)
	}
	if p.output == nil {
		logger.Panicf("output isn't set for pipeline %q", p.name)
	}

	p.output.Start(p.outputData.Config, p.capacity, p)

	for _, processor := range p.Processors {
		processor.start(p.output)
	}

	p.input.Start(p.inputData.Config, p, p.doneWg)

	go p.maintenance()
}

func (p *Pipeline) Stop() {
	logger.Infof("stopping pipeline %q, total: in=%d, processed=%d", p.name, p.totalIn.Load(), p.totalCommitted.Load())
	p.stopCh <- true

	logger.Infof("stopping pipeline %q processors count=%d", p.name, len(p.Processors))
	for _, processor := range p.Processors {
		processor.stop()
	}

	logger.Infof("stopping %q input", p.name)
	p.input.Stop()

	logger.Infof("stopping %q output", p.name)
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

func (p *Pipeline) Deprecate(sourceId SourceId) {
	count := 0
	p.eventsDefault.visit(func(e *Event) {
		if e.Source == sourceId {
			e.markDeprecated()
			count++
		}
	})

	p.eventsVIP.visit(func(e *Event) {
		if e.Source == sourceId {
			e.markDeprecated()
			count++
		}
	})

	logger.Infof("events marked as deprecated count=%d source=%d", count, sourceId)
}

func (p *Pipeline) In(sourceId SourceId, sourceName string, offset int64, size int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	p.totalIn.Inc()

	processorIndex := p.sourceIDToProcessor(sourceId)
	waitingSourceId := p.waitingProcessors[processorIndex].Load()

	var event *Event
	if waitingSourceId == uint64(sourceId) {
		event = p.eventsVIP.get()
	} else {
		event = p.eventsDefault.get()
	}

	json, err := event.ParseJSON(bytes)
	if err != nil {
		logger.Fatalf("wrong json offset=%d length=%d err=%s source=%d:%s json=%s", offset, len(bytes), err.Error(), sourceId, sourceName, bytes)
		return
	}

	stream := "default"
	streamBytes := json.GetStringBytes("stream")
	if streamBytes != nil {
		stream = string(streamBytes)
	}

	event.JSON = json
	event.Offset = offset + size
	event.Source = sourceId
	event.StreamName = StreamName(stream)
	event.SourceName = sourceName
	event.Size = len(bytes)

	p.getStream(sourceId, sourceName, StreamName(stream)).put(event)
}

func (p *Pipeline) getStream(sourceId SourceId, sourceName string, streamName StreamName) *stream {
	// fast path, stream have been already created
	p.streamsMu.RLock()
	st, has := p.streams[sourceId][streamName]
	p.streamsMu.RUnlock()
	if has {
		return st
	}

	// slow path, create new stream
	p.streamsMu.Lock()
	defer p.streamsMu.Unlock()
	st, has = p.streams[sourceId][streamName]
	if has {
		return st
	}

	_, has = p.streams[sourceId]
	if !has {
		p.streams[sourceId] = make(map[StreamName]*stream)
	}

	st = newStream(streamName, sourceId, p.Processors[p.sourceIDToProcessor(sourceId)])
	p.streams[sourceId][streamName] = st

	return st
}

func (p *Pipeline) Commit(event *Event) {
	p.commit(event, true)
}

func (p *Pipeline) commit(event *Event, notifyInput bool) {
	if p.sampleEvery != 0 && event.ID%p.sampleEvery == 0 {
		logger.Infof("pipeline %q final event sample: %s", p.name, event.JSON.String())
	}

	if notifyInput {
		p.input.Commit(event)
	}

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, event.JSON.String())
		p.eventLogMu.Unlock()
	}

	if event.poolID == eventsPoolDefault {
		p.eventsDefault.back(event)
	} else {
		p.eventsVIP.back(event)
	}

	p.totalCommitted.Inc()
}

func (p *Pipeline) nextMetricsGeneration() {
	metricsGen := strconv.Itoa(p.metricsGen)

	for _, processor := range p.Processors {
		processor.nextMetricsGeneration(metricsGen)
	}

	p.metricsGen++
	p.metricsGenTime = time.Now()
}

func (p *Pipeline) maintenance() {
	lastProcessed := int64(0)
	time.Sleep(maintenanceInterval)
	for {
		select {
		case <-p.stopCh:
			return
		default:
			//if p.eventsDefault.eventsCount == p.capacity {
			logger.Infof("========processor state dump========")
			for i, t := range p.Processors {
				t.dumpState(SourceId(p.waitingProcessors[i].Load()))
			}

			logger.Infof("========default events dump========")
			p.eventsDefault.visit(func(event *Event) {
				logger.Infof("event: index=%d, id=%d processor id=%d, stream=%d(%s)", event.poolIndex, event.ID, event.processorID, event.Source, event.StreamName)
			})
			logger.Infof("========vip events dump========")
			p.eventsVIP.visit(func(event *Event) {
				logger.Infof("event: index=%d, id=%d processor id=%d, stream=%d(%s)", event.poolIndex, event.ID, event.processorID, event.Source, event.StreamName)
			})
			//}

			processed := p.totalCommitted.Load()
			delta := processed - lastProcessed
			lastProcessed = processed
			rate := delta * int64(time.Second) / int64(maintenanceInterval)
			logger.Infof("pipeline %q stats for last %d seconds: processed=%d, rate=%d/sec, queue=%d/%d, total processed=%d, max log size=%d", p.name, maintenanceInterval/time.Second, delta, rate, p.eventsDefault.eventsCount, p.capacity, processed, p.eventsDefault.maxEventSize)

			if time.Now().Sub(p.metricsGenTime) > metricsGenInterval {
				p.nextMetricsGeneration()
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

func (p *Pipeline) getStreamEvent(stream *stream, shouldWait bool) *Event {
	if shouldWait {
		p.waitingProcessors[stream.processor.id].Swap(uint64(stream.sourceId))
		event := stream.waitGet()
		p.waitingProcessors[stream.processor.id].Swap(0)

		return event
	}

	return stream.instantGet()
}

func (p *Pipeline) GetEventLogItem(index int) string {
	if index >= len(p.eventLog) {
		logger.Fatalf("can't find log item with index %d", index)
	}
	return p.eventLog[index]
}

func (p *Pipeline) sourceIDToProcessor(id SourceId) int {
	// super random algorithm
	h := (14695981039346656037 ^ uint64(id)) * 1099511628211
	return int(h % uint64(len(p.Processors)))
}
