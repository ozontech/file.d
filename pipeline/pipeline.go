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
	defaultFieldValue   = "not_set"
	maintenanceInterval = time.Second * 5
	metricsGenInterval  = time.Minute * 10
)

type Head interface {
	In(sourceId SourceId, sourceName string, offset int64, size int64, bytes []byte)
	Deprecate(sourceId SourceId) int // mark source events in the pipeline as deprecated, it means that these events shouldn't update offsets
}

type Tail interface {
	Commit(event *Event) // notify input plugin that event is successfully processed and save offsets
}

type SourceId uint64
type StreamName string

type Pipeline struct {
	name      string
	capacity  int
	eventPool *eventPool

	input      InputPlugin
	inputData  *InputPluginData
	Processors []*processor
	output     OutputPlugin
	outputData *OutputPluginData

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

	readyStreams     []*stream
	readyStreamsMu   *sync.Mutex
	readyStreamsCond *sync.Cond

	// some debugging shit
	eventSample    string
	totalCommitted atomic.Int64
	totalIn        atomic.Int64
}

func New(name string, capacity int, processorsCount int, registry *prometheus.Registry) *Pipeline {
	logger.Infof("creating new pipeline with processors=%d capacity=%d", processorsCount, capacity)
	pipeline := &Pipeline{
		name:     name,
		capacity: capacity,

		streams:   make(map[SourceId]map[StreamName]*stream),
		streamsMu: &sync.RWMutex{},

		doneWg: &sync.WaitGroup{},

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop

		registry: registry,

		readyStreams:   make([]*stream, 0, 0),
		readyStreamsMu: &sync.Mutex{},
	}

	pipeline.readyStreamsCond = sync.NewCond(pipeline.readyStreamsMu)

	processors := make([]*processor, processorsCount)
	for i := 0; i < processorsCount; i++ {
		processors[i] = NewProcessor(i, pipeline)
	}
	pipeline.Processors = processors

	pipeline.eventPool = newEventPool(pipeline.capacity)

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

func (p *Pipeline) Deprecate(sourceId SourceId) int {
	count := 0
	p.eventPool.visit(func(e *Event) {
		if e.Source == sourceId {
			e.markDeprecated()
			count++
		}
	})

	return count
}

func (p *Pipeline) In(sourceId SourceId, sourceName string, offset int64, size int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	p.totalIn.Inc()

	event := p.eventPool.get()
	event.stage = eventStageHead

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

	st = newStream(streamName, sourceId, p)
	p.streams[sourceId][streamName] = st

	return st
}

func (p *Pipeline) attachToStream(processor *processor) *stream {
	p.readyStreamsMu.Lock()
	for len(p.readyStreams) == 0 {
		p.readyStreamsCond.Wait()
	}
	stream := p.readyStreams[0]
	stream.attach(processor)
	p.removeReadyStream(stream)
	p.readyStreamsMu.Unlock()

	return stream
}

func (p *Pipeline) addReadyStream(stream *stream) {
	p.readyStreamsMu.Lock()
	stream.readyIndex = len(p.readyStreams)
	p.readyStreams = append(p.readyStreams, stream)
	p.readyStreamsCond.Signal()
	p.readyStreamsMu.Unlock()
}

func (p *Pipeline) removeReadyStream(stream *stream) {
	if stream.readyIndex == -1 {
		logger.Panicf("why remove? stream isn't ready")
	}

	lastIndex := len(p.readyStreams) - 1
	if lastIndex == -1 {
		logger.Panicf("why remove? stream isn't in ready list")
	}
	index := stream.readyIndex
	p.readyStreams[index] = p.readyStreams[lastIndex]
	p.readyStreams[index].readyIndex = index
	p.readyStreams = p.readyStreams[:lastIndex]

	stream.readyIndex = -1
}

func (p *Pipeline) Commit(event *Event) {
	p.commit(event, true)
}

func (p *Pipeline) commit(event *Event, notifyInput bool) {
	event.stage = eventStageTail
	if notifyInput {
		if p.eventSample == "" {
			p.eventSample = event.JSON.String()
		}
		p.input.Commit(event)
	}

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, event.JSON.String())
		p.eventLogMu.Unlock()
	}

	event.stream.commit(event)
	p.eventPool.back(event)
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
			logger.Infof("pipeline %q final event sample: %s", p.name, p.eventSample)
			p.eventSample = ""

			if p.eventPool.eventsCount == p.capacity {
				p.streamsMu.Lock()
				logger.Infof("========streams======== ready=%d", len(p.readyStreams))
				for _, s := range p.streams {
					for _, stream := range s {
						if stream.isDetaching {
							logger.Infof("stream %d(%s) state=DETACHING, get offset=%d, commit offset=%d", stream.sourceId, stream.name, stream.getOffset, stream.commitOffset)
						}
						if stream.isAttached {
							logger.Infof("stream %d(%s) state=ATTACHED", stream.sourceId, stream.name)
						}
					}
				}
				p.streamsMu.Unlock()
				logger.Infof("========processor state dump========")
				for _, t := range p.Processors {
					t.dumpState()
				}

				logger.Infof("========default events dump========")
				p.eventPool.visit(func(event *Event) {
					logger.Infof("event: index=%d, id=%d, stream=%d(%s), stage=%s", event.poolIndex, event.ID, event.Source, event.StreamName, event.stageStr())
				})
			}

			processed := p.totalCommitted.Load()
			delta := processed - lastProcessed
			lastProcessed = processed
			rate := delta * int64(time.Second) / int64(maintenanceInterval)
			logger.Infof("pipeline %q stats for last %d seconds: processed=%d, rate=%d/sec, queue=%d/%d, total processed=%d, max log size=%d", p.name, maintenanceInterval/time.Second, delta, rate, p.eventPool.eventsCount, p.capacity, processed, p.eventPool.maxEventSize)

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

func (p *Pipeline) GetEventLogItem(index int) string {
	if index >= len(p.eventLog) {
		logger.Fatalf("can't find log item with index %d", index)
	}
	return p.eventLog[index]
}
