package pipeline

import (
	"math/rand"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const statsInfoReportInterval = time.Second * 5
const defaultCapacity = 1024

type Head interface {
	In(sourceId SourceId, sourceName string, offset int64, size int64, bytes []byte)
	DeprecateEvents(sourceId SourceId) // mark source events in the pipeline as deprecated, it means that these events shouldn't update offsets
}

type ActionController interface {
	Propagate()          // passes event to the next action in pipeline
	Next()               // requests next event from the same stream as current
	Commit(event *Event) // notifies input plugin that event is successfully processed
	Drop(event *Event)   // skip processing of event
}

type OutputController interface {
	Commit(event *Event) // tells that this event is processed
}

type SourceId uint64
type StreamName string

type Pipeline struct {
	name      string
	capacity  int
	eventPool *eventPool

	inputDescr  *InputPluginDescription
	input       InputPlugin
	Tracks      []*track
	outputDescr *OutputPluginDescription
	output      OutputPlugin

	streams   map[SourceId]map[StreamName]*stream
	streamsMu *sync.Mutex

	doneWg *sync.WaitGroup

	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex

	stopCh chan bool

	// some debugging shit
	eventsProcessed atomic.Int64
}

func New(name string, headsCount int, tracksCount int) *Pipeline {
	logger.Infof("creating new pipeline with heads=%d tracks=%d capacity=%d", headsCount, tracksCount, defaultCapacity)

	pipeline := &Pipeline{
		name:     name,
		capacity: int(defaultCapacity),

		doneWg: &sync.WaitGroup{},

		streams:   make(map[SourceId]map[StreamName]*stream),
		streamsMu: &sync.Mutex{},

		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop
	}

	tracks := make([]*track, tracksCount)
	for i := 0; i < tracksCount; i++ {
		tracks[i] = NewTrack(pipeline)
	}

	pipeline.eventPool = newEventPool(pipeline.capacity)
	pipeline.Tracks = tracks

	return pipeline
}

func (p *Pipeline) Start() {
	p.HandleEventFlowStart()

	if p.input == nil {
		logger.Panicf("input isn't set for pipeline %q", p.name)
	}
	if p.output == nil {
		logger.Panicf("output isn't set for pipeline %q", p.name)
	}

	p.output.Start(p.outputDescr.Config, p)

	for _, track := range p.Tracks {
		track.start(p.output)
	}

	p.input.Start(p.inputDescr.Config, p, p.doneWg)

	go p.reportStats()
}

func (p *Pipeline) Stop() {
	logger.Infof("stopping pipeline %s", p.name)
	p.stopCh <- true

	logger.Infof("stopping %d tracks of pipeline %s", len(p.Tracks), p.name)
	for _, track := range p.Tracks {
		track.stop()
	}

	p.streamsMu.Lock()
	for _, s := range p.streams {
		for _, st := range s {
			// unlock all waiting streams
			st.put(nil)
		}
	}
	p.streamsMu.Unlock()

	logger.Infof("stopping %s input", p.name)
	p.input.Stop()

	logger.Infof("stopping %s output", p.name)
	p.output.Stop()
}

func (p *Pipeline) DeprecateEvents(sourceId SourceId) {
	count := 0
	p.eventPool.visit(func(e *Event) {
		if e.Source == sourceId {
			e.markDeprecated()
			count++
		}
	})

	logger.Infof("events marked as deprecated count=%d source=%d", count, sourceId)
}

func (p *Pipeline) In(sourceId SourceId, sourceName string, from int64, delta int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	event, index := p.eventPool.get()
	if index == 0 {
		p.handleFirstEvent()
	}

	json, err := event.ParseJSON(bytes)
	if err != nil {
		logger.Fatalf("wrong json offset=%d length=%d err=%s source=%d:%s json=%s", from, len(bytes), err.Error(), sourceId, sourceName, bytes)
		return
	}

	streamName := "default"
	streamBytes := json.GetStringBytes("stream")
	if streamBytes != nil {
		streamName = string(streamBytes)
	}

	o, _ := json.Object()
	o.Set("details", event.JSONPool.NewString("filed"))

	event.JSON = json
	event.Offset = from + delta
	event.Source = sourceId
	event.Stream = StreamName(streamName)
	event.SourceName = sourceName
	event.SourceSize = len(bytes)
	event.raw = bytes

	p.pushToStream(event)
}

func (p *Pipeline) pushToStream(event *Event) {
	p.streamsMu.Lock()
	st, has := p.streams[event.Source][event.Stream]

	if !has {
		_, has := p.streams[event.Source]
		if !has {
			p.streams[event.Source] = make(map[StreamName]*stream)
		}
		st, has = p.streams[event.Source][event.Stream]
		if !has {
			// assign random track for new stream
			st = newStream(event.Stream, event.Source, p.Tracks[rand.Int()%len(p.Tracks)])
			p.streams[event.Source][event.Stream] = st
		}
	}
	p.streamsMu.Unlock()

	st.put(event)
}

func (p *Pipeline) Commit(event *Event) {
	p.commit(event, true)
}

func (p *Pipeline) commit(event *Event, notifyInput bool) {
	if notifyInput {
		p.input.Commit(event)
	}

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, string(event.raw))
		p.eventLogMu.Unlock()
	}

	if p.eventPool.back(event) == 0 {
		p.handleLastEvent()
	}

	p.eventsProcessed.Inc()
}

func (p *Pipeline) reportStats() {
	lastProcessed := int64(0)
	time.Sleep(statsInfoReportInterval)
	for {
		select {
		case <-p.stopCh:
			return
		default:
			processed := p.eventsProcessed.Load()
			delta := processed - lastProcessed
			rate := float32(delta) / float32(statsInfoReportInterval) * float32(time.Second)

			logger.Infof("pipeline %q stats for last %d seconds: processed=%d, rate=%.f/sec, queue=%d, total processed=%d, max log size=%d", p.name, statsInfoReportInterval/time.Second, delta, rate, p.eventPool.eventsCount, processed, p.eventPool.maxEventSize)

			lastProcessed = processed
			time.Sleep(statsInfoReportInterval)
		}
	}
}

func (p *Pipeline) SetInputPlugin(descr *InputPluginDescription) {
	p.inputDescr = descr
	p.input = descr.Plugin
}

func (p *Pipeline) SetOutputPlugin(descr *OutputPluginDescription) {
	p.outputDescr = descr
	p.output = descr.Plugin
}

func (p *Pipeline) EnableEventLog() {
	p.eventLogEnabled = true
}

func (p *Pipeline) handleFirstEvent() {
	p.doneWg.Add(1)
}

func (p *Pipeline) handleLastEvent() {
	p.doneWg.Done()
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

		processed := p.eventsProcessed.Load()
		//fs events may have delay, so wait for them
		time.Sleep(time.Millisecond * 100)
		//
		processed = p.eventsProcessed.Load() - processed
		if processed == 0 {
			break
		}
	}
}

func (p *Pipeline) EventsProcessed() int {
	return int(p.eventsProcessed.Load())
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
