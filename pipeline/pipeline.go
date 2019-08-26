package pipeline

import (
	"math/rand"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const statsInfoReportInterval = time.Second * 5
const defaultCapacity = 128

type Pipeline interface {
	GetHeads() []*Head
	GetDoneWg() *sync.WaitGroup
}

type SplitPipeline struct {
	name         string
	capacity     int
	wasProcessed bool

	heads       []*Head
	Tracks      []*Track
	splitBuffer *splitBuffer

	inputPlugin *PluginDescription

	doneWg          *sync.WaitGroup
	waitingForEvent *atomic.Bool

	eventLogEnabled bool
	eventLog        []string
	eventLogMu      sync.Mutex

	stopCh chan bool

	// some debugging shit
	eventsProcessed *atomic.Int64
}

func New(name string, headsCount int, tracksCount int) *SplitPipeline {
	logger.Infof("starting new pipeline with heads=%d tracks=%d capacity=%d", headsCount, tracksCount, defaultCapacity)

	pipeline := &SplitPipeline{
		name:     name,
		capacity: defaultCapacity,

		doneWg:          &sync.WaitGroup{},
		waitingForEvent: &atomic.Bool{},

		eventLog:   make([]string, 0, 128),
		eventLogMu: sync.Mutex{},

		stopCh: make(chan bool, 1), //non-zero channel cause we don't want to wait goroutine to stop

		eventsProcessed: atomic.NewInt64(0),
	}

	splitBuffer := newSplitBuffer(pipeline)

	heads := make([]*Head, headsCount)
	for i := 0; i < headsCount; i++ {
		heads[i] = NewHead(splitBuffer)
	}

	tracks := make([]*Track, tracksCount)
	for i := 0; i < tracksCount; i++ {
		tracks[i] = NewTrack(pipeline)
	}

	pipeline.splitBuffer = splitBuffer
	pipeline.heads = heads
	pipeline.Tracks = tracks

	return pipeline
}

func (p *SplitPipeline) Start() {
	p.HandleEventFlowStart()

	p.inputPlugin.Plugin.(InputPlugin).Start(p.inputPlugin.Config, p)

	for _, track := range p.Tracks {
		track.start()
	}

	go p.reportStats()
}

func (p *SplitPipeline) Stop() {
	logger.Infof("stopping pipeline %s", p.name)
	p.stopCh <- true

	logger.Infof("stopping %d tracks of pipeline %s", len(p.Tracks), p.name)
	for _, track := range p.Tracks {
		track.stop()
	}

	logger.Infof("stopping %s input", p.name)
	p.inputPlugin.Plugin.(InputPlugin).Stop()
}

func (p *SplitPipeline) commit(event *Event) {
	p.splitBuffer.commit(event)

	p.eventsProcessed.Inc()
	p.wasProcessed = true

	if p.eventLogEnabled {
		p.eventLogMu.Lock()
		defer p.eventLogMu.Unlock()

		p.eventLog = append(p.eventLog, string(event.raw))
	}
}

func (p *SplitPipeline) SetInputPlugin(plugin *PluginDescription) {
	p.inputPlugin = plugin
}

func (p *SplitPipeline) EventsProcessed() int {
	return int(p.eventsProcessed.Load())
}

func (p *SplitPipeline) EnableEventLog() {
	p.eventLogEnabled = true
}

func (p *SplitPipeline) attachStream(stream *stream) {
	// Assign random pipeline track for stream to have uniform event distribution
	track := p.Tracks[rand.Int()%len(p.Tracks)]
	track.addStream(stream)
}

func (p *SplitPipeline) reportStats() {
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

			logger.Infof("stats for last %d seconds: processed=%d, rate=%.f/sec, queue=%d", statsInfoReportInterval/time.Second, delta, rate, p.splitBuffer.eventsCount)

			lastProcessed = processed
			time.Sleep(statsInfoReportInterval)
		}
	}
}

func (p *SplitPipeline) handleFirstEvent() {
	p.doneWg.Add(1)
}

func (p *SplitPipeline) handleLastEvent() {
	p.doneWg.Done()
}

func (p *SplitPipeline) HandleEventFlowStart() {
	p.doneWg.Add(1)
}

func (p *SplitPipeline) HandleEventFlowFinish() {
	// fs events may have delay, so wait for them
	time.Sleep(time.Millisecond * 100)
	p.doneWg.Done()
}

func (p *SplitPipeline) WaitUntilDone() {
	for {
		p.doneWg.Wait()
		//fs events may have delay, so wait for them
		time.Sleep(time.Millisecond * 100)
		//
		if !p.wasProcessed {
			break
		}
		p.wasProcessed = false
	}
}

func (p *SplitPipeline) GetEventLogLength() int {
	return len(p.eventLog)
}

func (p *SplitPipeline) GetEventLogItem(index int) string {
	if index >= len(p.eventLog) {
		logger.Fatalf("Can't find log item with index %d", index)
	}
	return p.eventLog[index]
}

func (p *SplitPipeline) GetHeads() []*Head {
	return p.heads
}

func (p *SplitPipeline) GetDoneWg() *sync.WaitGroup {
	return p.doneWg
}
