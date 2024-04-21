package pipeline

import (
	"fmt"
	"sync"

	"github.com/ozontech/file.d/logger"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type Event struct {
	kind Kind

	children []*Event

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
	eventStagePool = iota
	eventStageInput
	eventStageStream
	eventStageProcessor
	eventStageOutput
)

type Kind byte

const (
	EventKindRegular Kind = iota
	eventKindChild
	eventKindChildParent
	EventKindTimeout
	EventKindUnlock
)

func (k Kind) String() string {
	switch k {
	case EventKindRegular:
		return "REGULAR"
	case EventKindTimeout:
		return "TIMEOUT"
	case eventKindChildParent:
		return "PARENT"
	case eventKindChild:
		return "CHILD"
	case EventKindUnlock:
		return "UNLOCK"
	}
	return "UNKNOWN"
}

type eventStage int

func newEvent() *Event {
	return &Event{
		Root: insaneJSON.Spawn(),
		Buf:  make([]byte, 0, 1024),
	}
}

func newTimeoutEvent(stream *stream) *Event {
	event := &Event{
		Root:       nil,
		stream:     stream,
		SeqID:      stream.commitSeq.Load(),
		SourceID:   SourceID(stream.streamID),
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
		SeqID:      stream.commitSeq.Load(),
		SourceID:   SourceID(stream.streamID),
		SourceName: "unlock",
		streamName: stream.name,
	}

	event.SetUnlockKind()

	return event
}

func (e *Event) reset(avgEventSize int) {
	if e.Size > avgEventSize {
		e.Root.ReleaseBufMem()
	}

	if cap(e.Buf) > 4096 {
		e.Buf = make([]byte, 0, 1024)
	}

	if e.Root.PoolSize() > DefaultJSONNodePoolSize*4 {
		e.Root.ReleasePoolMem()
	}

	e.Buf = e.Buf[:0]
	e.next = nil
	e.action = 0
	e.stream = nil
	e.children = e.children[:0]
	e.kind = EventKindRegular
}

func (e *Event) StreamNameBytes() []byte {
	return StringToByteUnsafe(string(e.streamName))
}

func (e *Event) IsRegularKind() bool {
	return e.kind == EventKindRegular
}

func (e *Event) IsUnlockKind() bool {
	return e.kind == EventKindUnlock
}

func (e *Event) SetUnlockKind() {
	e.kind = EventKindUnlock
}

func (e *Event) IsIgnoreKind() bool {
	return e.kind == EventKindUnlock
}

func (e *Event) SetTimeoutKind() {
	e.kind = EventKindTimeout
}

func (e *Event) IsTimeoutKind() bool {
	return e.kind == EventKindTimeout
}

func (e *Event) SetChildKind() {
	e.kind = eventKindChild
}

func (e *Event) IsChildKind() bool {
	return e.kind == eventKindChild
}

func (e *Event) SetChildParentKind() {
	e.kind = eventKindChildParent
}

func (e *Event) IsChildParentKind() bool {
	return e.kind == eventKindChildParent
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

func (e *Event) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("kind=%s, action=%d, source=%d/%s, stream=%s, stage=%s, json=%s", e.kind.String(), e.action, e.SourceID, e.SourceName, e.streamName, e.stageStr(), e.Root.EncodeToString())
}

// channels are slower than this implementation by ~20%
type eventPool struct {
	avgEventSize int
	capacity     int
	obj          []*Event
	freeptr      int
	cond         sync.Cond
}

func newEventPool(capacity, avgEventSize int) *eventPool {
	pool := &eventPool{
		avgEventSize: avgEventSize,
		capacity:     capacity,
		obj:          make([]*Event, capacity),
		freeptr:      capacity - 1,
		cond:         sync.Cond{L: &sync.Mutex{}},
	}

	for i := 0; i < capacity; i++ {
		pool.obj[i] = newEvent()
	}

	return pool
}

func (p *eventPool) get() *Event {
	p.cond.L.Lock()
	for p.freeptr < 0 {
		p.cond.Wait()
	}
	event := p.obj[p.freeptr]
	p.freeptr--
	p.cond.L.Unlock()

	event.stage = eventStageInput

	return event
}

func (p *eventPool) back(event *Event) {
	event.stage = eventStagePool
	event.reset(p.avgEventSize)
	p.cond.L.Lock()
	p.freeptr++
	p.obj[p.freeptr] = event
	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *eventPool) size() int {
	p.cond.L.Lock()
	s := p.freeptr + 1
	p.cond.L.Unlock()
	return s
}

func (p *eventPool) dump() string {
	out := logger.Cond(len(p.obj) == 0, logger.Header("no events"), func() string {
		o := logger.Header("events")
		for _, event := range p.obj {
			eventStr := event.String()
			if eventStr == "" {
				eventStr = "nil"
			}
			o += eventStr + "\n"
		}

		return o
	})

	return out
}
