package pipeline

import (
	"sync"

	"github.com/alecthomas/units"
	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

var eventGCSizeThreshold = 4 * units.Kibibyte

type Event struct {
	Root      *insaneJSON.Root
	poolIndex int
	next      *Event
	stream    *stream

	SeqID      uint64
	Offset     int64
	Source     SourceId
	SourceName string
	StreamName StreamName
	Size       int

	isDeprecated atomic.Bool
	shouldGC     bool

	// some debugging shit
	maxSize int
	stage   eventStage
}

const (
	eventStagePool      = 0
	eventStageHead      = 1
	eventStageStream    = 2
	eventStageProcessor = 3
	eventStageOutput    = 4
	eventStageTail      = 5
)

type eventStage int

func newEvent(poolIndex int) *Event {
	return &Event{
		poolIndex: poolIndex,
		Root:      insaneJSON.Spawn(),
	}
}

func (e *Event) stageStr() string {
	switch e.stage {
	case eventStagePool:
		return "POOL"
	case eventStageHead:
		return "HEAD"
	case eventStageStream:
		return "STREAM"
	case eventStageProcessor:
		return "PROCESSOR"
	case eventStageOutput:
		return "OUTPUT"
	case eventStageTail:
		return "TAIL"
	default:
		return "UNKNOWN"
	}
}
func (e *Event) reset() {
	if e.shouldGC {
		e.Root.ReleaseMem()
		e.shouldGC = false
	}

	e.stage = eventStageHead
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

func (e *Event) Marshal(out []byte) ([]byte, int) {
	l := len(out)
	out = e.Root.Encode(out)

	size := len(out) - l
	// event is going to be super big, lets GC it
	e.shouldGC = size > int(eventGCSizeThreshold)

	if size > e.maxSize {
		e.maxSize = size
	}

	return out, l
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

func (p *eventPool) get(bytes []byte) (*Event, error) {
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
	return event.parseJSON(bytes)
}

func (p *eventPool) back(event *Event) {
	p.mu.Lock()

	event.stage = eventStagePool
	p.eventsCount--
	if p.eventsCount == -1 {
		logger.Panicf("event pool is full, why back()? id=%d index=%d", event.SeqID, event.poolIndex)
	}

	currentIndex := event.poolIndex
	lastIndex := p.eventsCount

	last := p.events[lastIndex]

	// exchange event with last one to place back to pool
	p.events[currentIndex] = last
	p.events[lastIndex] = event

	event.poolIndex = lastIndex
	last.poolIndex = currentIndex

	p.cond.Signal()
	p.mu.Unlock()
}
