package pipeline

import (
	"fmt"
	"sync"

	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

var eventSizeGCThreshold = 4 * 1024

type Event struct {
	kind atomic.Int32

	Root *insaneJSON.Root
	Buf  []byte

	SeqID      uint64
	Offset     int64
	SourceID   SourceID
	SourceName string
	StreamName StreamName
	Size       int // last known event size, it may not be actual

	index  int
	action int
	next   *Event
	stream *stream

	// some debugging shit
	stage eventStage
}

const (
	eventStagePool      = 0
	eventStageInput     = 1
	eventStageStream    = 2
	eventStageProcessor = 3
	eventStageOutput    = 4
	eventStageBack      = 5

	eventKindRegular    eventKind = 0
	eventKindDeprecated eventKind = 1
	eventKindTimeout    eventKind = 2
)

type eventStage int
type eventKind int32

func newEvent(poolIndex int) *Event {
	return &Event{
		index: poolIndex,
		Root:  insaneJSON.Spawn(),
		Buf:   make([]byte, 0, 1024),
	}
}

func newTimoutEvent(stream *stream) *Event {
	//todo: use pooling here?
	event := &Event{
		index:      -1,
		Root:       insaneJSON.Spawn(),
		stream:     stream,
		SeqID:      stream.commitID,
		SourceID:   stream.sourceID,
		SourceName: stream.sourceName,
		StreamName: stream.name,
	}

	event.ToTimeoutKind()

	return event
}

func (e *Event) reset() {
	if e.Size > eventSizeGCThreshold {
		e.Root.ReleaseBufMem()
	}

	if cap(e.Buf) > 4096 {
		e.Buf = make([]byte, 0, 1024)
	}

	if e.Root.PoolSize() > DefaultNodePoolSize*4 {
		e.Root.ReleasePoolMem()
	}

	e.Buf = e.Buf[:0]
	e.stage = eventStageInput
	e.next = nil
	e.action = 0
	e.stream = nil
	e.kind.Swap(int32(eventKindRegular))
}

func (e *Event) ToDeprecatedKind() {
	e.kind.Swap(int32(eventKindDeprecated))
}

func (e *Event) IsDeprecatedKind() bool {
	return e.kind.Load() == int32(eventKindDeprecated)
}

func (e *Event) ToTimeoutKind() {
	e.kind.Swap(int32(eventKindTimeout))
}

func (e *Event) IsTimeoutKind() bool {
	return e.kind.Load() == int32(eventKindTimeout)
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
	e.Size = len(outBuf) - l

	return outBuf, l
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

func (e *Event) kindStr() string {
	switch eventKind(e.kind.Load()) {
	case eventKindRegular:
		return "REGULAR"
	case eventKindDeprecated:
		return "DEPRECATED"
	case eventKindTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

func (e *Event) String() string {
	return fmt.Sprintf("id=%d, index=%d kind=%s, action=%d, source=%d/%s, stream=%s, stage=%s, json=%s", e.SeqID, e.index, e.kindStr(), e.action, e.SourceID, e.SourceName, e.StreamName, e.stageStr(), e.Root.EncodeToString())
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

	event.reset()
	return event.parseJSON(json)
}

func (p *eventPool) back(event *Event) {
	p.mu.Lock()
	event.stage = eventStageBack

	if event.Size > p.maxEventSize {
		p.maxEventSize = event.Size
	}

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

func (p *eventPool) dump() string {
	out := logger.Cond(len(p.events) == 0, logger.Header("no events"), func() string {
		o := logger.Header("events")
		p.visit(func(event *Event) {
			o += event.String() + "\n"
		})

		return o
	})

	return out
}
