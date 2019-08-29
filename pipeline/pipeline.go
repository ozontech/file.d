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
	Accept(sourceId SourceId, additional string, from int64, delta int64, bytes []byte)
}

type ActionController interface {
	Propagate()          // passes event to the next action in pipeline
	Next()               // requests next event from the same stream as current
	Commit(event *Event) // tells that this event is processed
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

	inputDescr  *InputPluginDescription  // first input plugin reads something
	input       InputPlugin              // then plugin push logs to the pipeline head and it decode log to event
	Tracks      []*track                 // then events splits by streams
	outputDescr *OutputPluginDescription // each stream is attached to the pipeline track(pipeline instance), track launches action plugins on event
	output      OutputPlugin             // output plugin is the final point of pipeline it can't propagate event to next plugin it has only commit method

	streams   map[SourceId]map[StreamName]*stream
	streamsMu *sync.RWMutex

	doneWg *sync.WaitGroup

	eventLogEnabled bool
	eventLog        []string
	eventLogMu      *sync.Mutex

	stopCh chan bool

	// some debugging shit
	eventsProcessed *atomic.Int64
}

func New(name string, headsCount int, tracksCount int) *Pipeline {
	logger.Infof("starting new pipeline with heads=%d tracks=%d capacity=%d", headsCount, tracksCount, defaultCapacity)

	pipeline := &Pipeline{
		name:     name,
		capacity: defaultCapacity,

		doneWg: &sync.WaitGroup{},

		streams:   make(map[SourceId]map[StreamName]*stream),
		streamsMu: &sync.RWMutex{},


		eventLog:   make([]string, 0, 128),
		eventLogMu: &sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't wanna wait goroutine to stop

		eventsProcessed: atomic.NewInt64(0),
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

	logger.Infof("stopping %s input", p.name)
	p.input.Stop()

	logger.Infof("stopping %s output", p.name)
	p.output.Stop()
}

func (p *Pipeline) Accept(sourceId SourceId, additional string, from int64, delta int64, bytes []byte) {
	if len(bytes) == 0 {
		return
	}

	event, index := p.eventPool.get()
	if index == 0 {
		p.handleFirstEvent()
	}

	streamName := "default"

	json, err := event.parser.ParseBytes(bytes)
	if err != nil {
		logger.Fatalf("wrong json %s at offset %d", string(bytes), from)
		return
	}

	if json.Get("stream") != nil {
		streamName = json.Get("stream").String()
	}

	event.Value = json
	event.Offset = from + delta
	event.SourceId = sourceId
	event.Stream = StreamName(streamName)
	event.Additional = additional

	event.raw = bytes

	p.pushToStream(event)
}

func (p *Pipeline) pushToStream(event *Event) {
	p.streamsMu.RLock()
	st, has := p.streams[event.SourceId][event.Stream]
	p.streamsMu.RUnlock()

	if !has {
		p.streamsMu.Lock()
		_, has := p.streams[event.SourceId]
		if !has {
			p.streams[event.SourceId] = make(map[StreamName]*stream)
		}
		st, has = p.streams[event.SourceId][event.Stream]
		if !has {
			// assign random track for new stream
			st = newStream(event.Stream, event.SourceId, p.Tracks[rand.Int()%len(p.Tracks)])
			p.streams[event.SourceId][event.Stream] = st
		}
		p.streamsMu.Unlock()
	}

	st.put(event)
}

func (p *Pipeline) Commit(event *Event) {
	p.input.Commit(event)
	eventIndex := p.eventPool.back(event)

	p.eventsProcessed.Inc()

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		p.eventLog = append(p.eventLog, string(event.raw))
		p.eventLogMu.Unlock()
	}

	if eventIndex == 0 {
		p.handleLastEvent()
	}
}

func (p *Pipeline) reportStats() {
	lastProcessed := p.eventsProcessed.Load()
	time.Sleep(statsInfoReportInterval)
	for {
		select {
		case <-p.stopCh:
			return
		default:
			processed := p.eventsProcessed.Load()
			delta := processed - lastProcessed
			rate := float32(delta) / float32(statsInfoReportInterval) * float32(time.Second)

			logger.Infof("pipeline %q stats for last %d seconds: processed=%d, rate=%.f/sec, queue=%d", p.name, statsInfoReportInterval/time.Second, delta, rate, p.eventPool.eventsCount)

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

		events := p.eventsProcessed.Load()
		//fs events may have delay, so wait for them
		time.Sleep(time.Millisecond * 100)
		//
		eventsNew := p.eventsProcessed.Load()
		if events == eventsNew {
			break
		}
		eventsNew = events
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
