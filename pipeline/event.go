package pipeline

import (
	"sync"

	"github.com/alecthomas/units"
	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

var logCleanUpSize = 4 * units.Kibibyte

type Event struct {
	poolIndex int
	poolID    int
	next      *Event

	ID         uint64
	JSON       *fastjson.Value
	JSONPool   *fastjson.Arena
	Offset     int64
	Source     SourceId
	SourceName string
	StreamName StreamName
	Size       int

	deprecated    atomic.Bool
	shouldCleanUp bool

	parsers      []*fastjson.Parser
	parsersCount int

	// some debugging shit
	maxSize     int
	processorID int
}

func newEvent(poolID int, poolIndex int) *Event {
	return &Event{
		parsers:   make([]*fastjson.Parser, 0, 0),
		JSONPool:  &fastjson.Arena{},
		poolID:    poolID,
		poolIndex: poolIndex,
	}
}

func (e *Event) reset() {
	// reallocate big objects
	if e.shouldCleanUp {
		e.shouldCleanUp = false
		e.JSONPool = &fastjson.Arena{}
		e.parsers = make([]*fastjson.Parser, 0, 0)
	}

	e.next = nil
	e.JSONPool.Reset()
	e.parsersCount = 0
	e.deprecated.Swap(false)
}

func (e *Event) IsActual() bool {
	return e.deprecated.Load() == false
}

func (e *Event) markDeprecated() {
	e.deprecated.Swap(true)
}

func (e *Event) ParseJSON(json []byte) (*fastjson.Value, error) {
	if e.parsersCount+1 > len(e.parsers) {
		e.parsers = append(e.parsers, &fastjson.Parser{})
	}
	parser := e.parsers[e.parsersCount]
	e.parsersCount++

	return parser.ParseBytes(json)
}

func (e *Event) Marshal(out []byte) ([]byte, int) {
	l := len(out)
	out = e.JSON.MarshalTo(out)

	size := len(out) - l
	// event is going to be super big, lets GC it
	e.shouldCleanUp = size > int(logCleanUpSize)

	if size > e.maxSize {
		e.maxSize = size
	}

	return out, l
}

// channels are slower than this implementation by ~20%
type eventPool struct {
	id       int
	eventSeq uint64
	capacity int

	eventsCount int
	events      []*Event

	mu   *sync.Mutex
	cond *sync.Cond

	maxEventSize int
}

func newEventPool(id int, capacity int) *eventPool {
	eventPool := &eventPool{
		id:       id,
		capacity: capacity,
		events:   make([]*Event, capacity, capacity),
		mu:       &sync.Mutex{},
	}

	eventPool.cond = sync.NewCond(eventPool.mu)

	for i := 0; i < capacity; i++ {
		eventPool.events[i] = newEvent(id, i)
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

func (p *eventPool) get() *Event {
	p.mu.Lock()

	for p.eventsCount >= p.capacity {
		p.cond.Wait()
	}

	index := p.eventsCount
	event := p.events[index]
	event.ID = p.eventSeq

	p.eventsCount++
	p.eventSeq++

	p.mu.Unlock()

	event.reset()
	if event.maxSize > p.maxEventSize {
		p.maxEventSize = event.maxSize
	}
	return event
}

func (p *eventPool) back(event *Event) {
	p.mu.Lock()

	p.eventsCount--
	if p.eventsCount == -1 {
		logger.Panicf("event pool is full, why back()? id=%d index=%d", event.ID, event.poolIndex)
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
