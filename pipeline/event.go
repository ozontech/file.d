package pipeline

import (
	"fmt"
	"sync"

	"github.com/alecthomas/units"
	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

var eventSizeGCThreshold = 4 * units.Kibibyte

type Event struct {
	Root *insaneJSON.Root
	Buf  []byte

	SeqID      uint64
	Offset     int64
	SourceID   SourceID
	SourceName string
	StreamName StreamName
	Size       int

	index        int
	next         *Event
	stream       *stream
	isDeprecated atomic.Bool
	shouldGC     bool

	// some debugging shit
	maxSize int
	stage   eventStage
}

const (
	eventStagePool      = 0
	eventStageInput     = 1
	eventStageStream    = 2
	eventStageProcessor = 3
	eventStageOutput    = 4
	eventStageBack      = 5
)

type eventStage int

func newEvent(poolIndex int) *Event {
	return &Event{
		index: poolIndex,
		Root:  insaneJSON.Spawn(),
		Buf:   make([]byte, 0, 1024),
	}
}

func (e *Event) stageStr() string {
	switch e.stage {
	case eventStagePool:
		return "POOL"
	case eventStageInput:
		return "INPUT"
	case eventStageStream:
		return "STREAM"
	case eventStageProcessor:
		return "PROCESSOR"
	case eventStageOutput:
		return "OUTPUT"
	case eventStageBack:
		return "BACK"
	default:
		return "UNKNOWN"
	}
}

func (e *Event) reset() {
	if e.shouldGC {
		e.Root.ReleaseMem()
		e.Buf = make([]byte, 0, 1024)
		e.shouldGC = false
	}

	e.Buf = e.Buf[:0]
	e.stage = eventStageInput
	e.next = nil
	e.isDeprecated.Swap(false)
}

func (e *Event) deprecate() {
	e.isDeprecated.Swap(true)
}

func (e *Event) IsActual() bool {
	return e.isDeprecated.Load() == false
}

func (e *Event) parseJSON(json []byte) (*Event, error) {
	err := e.Root.DecodeBytes(json)
	if err != nil {
		return e, err
	}
	return e, nil
}

func (e *Event) SubparseJSON(json []byte) (*insaneJSON.Node, error) {
	return e.Root.DecodeBytesAdditional(json)
}

func (e *Event) Encode(outBuf []byte) ([]byte, int) {
	l := len(outBuf)
	outBuf = e.Root.Encode(outBuf)

	size := len(outBuf) - l
	// event is going to be super big, lets GC it
	e.shouldGC = size > int(eventSizeGCThreshold)

	if size > e.maxSize {
		e.maxSize = size
	}

	return outBuf, l
}

// channels are slower than this implementation by ~20%
type eventPool struct {
	eventSeq uint64
	capacity int

	eventsCount int
	events      []*Event

	mu   *sync.Mutex
	cond *sync.Cond

	maxEventSize int
}

func newEventPool(capacity int) *eventPool {
	eventPool := &eventPool{
		capacity: capacity,
		mu:       &sync.Mutex{},
	}

	eventPool.cond = sync.NewCond(eventPool.mu)

	for i := 0; i < capacity; i++ {
		eventPool.events = append(eventPool.events, newEvent(i))
	}

	return eventPool
}

func (p *eventPool) visit(fn func(*Event)) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < p.eventsCount; i++ {
		fn(p.events[i])
	}
}

func (p *eventPool) get(json []byte) (*Event, error) {
	p.mu.Lock()

	for p.eventsCount >= p.capacity {
		p.cond.Wait()
	}

	index := p.eventsCount
	event := p.events[index]
	event.SeqID = p.eventSeq

	p.eventsCount++
	p.eventSeq++

	p.mu.Unlock()

	if event.maxSize > p.maxEventSize {
		p.maxEventSize = event.maxSize
	}

	event.reset()
	return event.parseJSON(json)
}

func (p *eventPool) back(event *Event) {
	p.mu.Lock()

	event.stage = eventStagePool
	p.eventsCount--
	if p.eventsCount == -1 {
		logger.Panicf("event pool is full, why back()? id=%d index=%d", event.SeqID, event.index)
	}

	currentIndex := event.index
	lastIndex := p.eventsCount

	last := p.events[lastIndex]

	// exchange event with last one to place back to pool
	p.events[currentIndex] = last
	p.events[lastIndex] = event

	event.index = lastIndex
	last.index = currentIndex

	p.cond.Signal()
	p.mu.Unlock()
}

func (p *eventPool) dump() (result string) {
	result = logger.Cond(len(p.events) == 0, logger.Header("no events"), func() (result string) {
		result = logger.Header("events")
		p.visit(func(event *Event) {
			result += fmt.Sprintf("index=%d, id=%d, stream=%d(%s), stage=%s\n", event.index, event.SeqID, event.SourceID, event.StreamName, event.stageStr())
		})

		return result
	})

	return result
}
