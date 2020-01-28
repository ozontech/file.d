package pipeline

import (
	"fmt"
	"sync"

	insaneJSON "github.com/vitkovskii/insane-json"
	"gitlab.ozon.ru/sre/file-d/logger"
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
	streamName StreamName
	Size       int // last known event size, it may not be actual

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

	eventKindRegular int32 = 0
	eventKindIgnore  int32 = 1
	eventKindTimeout int32 = 2
	eventKindUnlock  int32 = 3
)

type eventStage int

func newEvent() *Event {
	return &Event{
		Root: insaneJSON.Spawn(),
		Buf:  make([]byte, 0, 1024),
	}
}

func newTimoutEvent(stream *stream) *Event {
	event := &Event{
		Root:       insaneJSON.Spawn(),
		stream:     stream,
		SeqID:      stream.commitSeq,
		SourceID:   stream.sourceID,
		SourceName: "timeout",
		streamName: stream.name,
	}

	event.SetTimeoutKind()

	return event
}

func unlockEvent(stream *stream) *Event {
	event := &Event{
		Root:       nil,
		stream:     stream,
		SeqID:      stream.commitSeq,
		SourceID:   stream.sourceID,
		SourceName: "unlock",
		streamName: stream.name,
	}

	event.SetUnlockKind()

	return event
}

func (e *Event) reset() {
	if e.Size > eventSizeGCThreshold {
		e.Root.ReleaseBufMem()
	}

	if cap(e.Buf) > 4096 {
		e.Buf = make([]byte, 0, 1024)
	}

	if e.Root.PoolSize() > DefaultJSONNodePoolSize*4 {
		e.Root.ReleasePoolMem()
	}

	e.Buf = e.Buf[:0]
	e.stage = eventStageInput
	e.next = nil
	e.action = 0
	e.stream = nil
	e.kind.Swap(eventKindRegular)
}

func (e *Event) StreamNameBytes() []byte {
	return StringToByteUnsafe(string(e.streamName))
}

func (e *Event) IsRegularKind() bool {
	return e.kind.Load() == eventKindRegular
}

func (e *Event) SetIgnoreKind() {
	e.kind.Swap(eventKindIgnore)
}

func (e *Event) IsUnlockKind() bool {
	return e.kind.Load() == eventKindUnlock
}

func (e *Event) SetUnlockKind() {
	e.kind.Swap(eventKindUnlock)
}

func (e *Event) IsIgnoreKind() bool {
	return e.kind.Load() == eventKindUnlock
}

func (e *Event) SetTimeoutKind() {
	e.kind.Swap(eventKindTimeout)
}

func (e *Event) IsTimeoutKind() bool {
	return e.kind.Load() == eventKindTimeout
}

func (e *Event) parseJSON(json []byte) error {
	return e.Root.DecodeBytes(json)
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
	default:
		return "UNKNOWN"
	}
}

func (e *Event) kindStr() string {
	switch e.kind.Load() {
	case eventKindRegular:
		return "REGULAR"
	case eventKindIgnore:
		return "DEPRECATED"
	case eventKindTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

func (e *Event) String() string {
	return fmt.Sprintf("kind=%s, action=%d, source=%d/%s, stream=%s, stage=%s, json=%s", e.kindStr(), e.action, e.SourceID, e.SourceName, e.streamName, e.stageStr(), e.Root.EncodeToString())
}

// channels are slower than this implementation by ~20%
// todo: may we use here some lock-free structures?

type eventPool struct {
	capacity int

	eventsCount int
	events      []*Event

	mu   *sync.Mutex
	cond *sync.Cond
}

func newEventPool(capacity int) *eventPool {
	eventPool := &eventPool{
		capacity:    capacity,
		eventsCount: capacity,
		mu:          &sync.Mutex{},
	}

	eventPool.cond = sync.NewCond(eventPool.mu)

	for i := 0; i < capacity; i++ {
		eventPool.events = append(eventPool.events, newEvent())
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

	for p.eventsCount == 0 {
		p.cond.Wait()
	}
	p.eventsCount--
	event := p.events[p.eventsCount]

	p.mu.Unlock()

	event.reset()
	return event
}

func (p *eventPool) back(event *Event) {
	p.mu.Lock()
	event.stage = eventStagePool
	p.events[p.eventsCount] = event
	p.eventsCount++
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
