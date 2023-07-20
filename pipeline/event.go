package pipeline

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/atomic"
)

type Event struct {
	kind atomic.Int32

	Root *insaneJSON.Root
	Buf  []byte

	SeqID  uint64
	Offset int64
	// Some input plugins have string offset (t.g. journalctl)
	OffsetString string
	SourceID     SourceID
	SourceName   string
	streamName   StreamName
	Size         int // last known event size, it may not be actual

	action atomic.Int64
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
)

type Kind byte

const (
	EventKindRegular Kind = iota
	EventKindTimeout
	EventKindUnlock
)

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
	e.stage = eventStageInput
	e.next = nil
	e.action = atomic.Int64{}
	e.stream = nil
	e.kind.Swap(int32(EventKindRegular))
}

func (e *Event) StreamNameBytes() []byte {
	return StringToByteUnsafe(string(e.streamName))
}

func (e *Event) IsRegularKind() bool {
	return Kind(e.kind.Load()) == EventKindRegular
}

func (e *Event) IsUnlockKind() bool {
	return Kind(e.kind.Load()) == EventKindUnlock
}

func (e *Event) SetUnlockKind() {
	e.kind.Swap(int32(EventKindUnlock))
}

func (e *Event) IsIgnoreKind() bool {
	return Kind(e.kind.Load()) == EventKindUnlock
}

func (e *Event) SetTimeoutKind() {
	e.kind.Swap(int32(EventKindTimeout))
}

func (e *Event) IsTimeoutKind() bool {
	return Kind(e.kind.Load()) == EventKindTimeout
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
	switch Kind(e.kind.Load()) {
	case EventKindRegular:
		return "REGULAR"
	case EventKindTimeout:
		return "TIMEOUT"
	default:
		return "UNKNOWN"
	}
}

func (e *Event) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("kind=%s, action=%d, source=%d/%s, stream=%s, stage=%s, json=%s", e.kindStr(), e.action.Load(), e.SourceID, e.SourceName, e.streamName, e.stageStr(), e.Root.EncodeToString())
}

func (e *Event) CopyTo(event *Event) {
	*event = *e
}

// channels are slower than this implementation by ~20%
type eventPool struct {
	capacity int

	avgEventSize int
	inUseEvents  atomic.Int64
	getCounter   atomic.Int64
	backCounter  atomic.Int64
	events       []*Event
	free1        []atomic.Bool
	free2        []atomic.Bool

	getMu   *sync.Mutex
	getCond *sync.Cond
}

func newEventPool(capacity, avgEventSize int) *eventPool {
	eventPool := &eventPool{
		avgEventSize: avgEventSize,
		capacity:     capacity,
		getMu:        &sync.Mutex{},
		backCounter:  *atomic.NewInt64(int64(capacity)),
	}

	eventPool.getCond = sync.NewCond(eventPool.getMu)

	for i := 0; i < capacity; i++ {
		eventPool.free1 = append(eventPool.free1, *atomic.NewBool(true))
		eventPool.free2 = append(eventPool.free2, *atomic.NewBool(true))
		eventPool.events = append(eventPool.events, newEvent())
	}

	return eventPool
}

const maxTries = 3

func (p *eventPool) get() *Event {
	x := (p.getCounter.Inc() - 1) % int64(p.capacity)
	var tries int
	for {
		if x < p.backCounter.Load() {
			// fast path
			if p.free1[x].CAS(true, false) {
				break
			}
			if p.free1[x].CAS(true, false) {
				break
			}
			if p.free1[x].CAS(true, false) {
				break
			}
		}
		tries++
		if tries%maxTries != 0 {
			// slow path
			runtime.Gosched()
		} else {
			// slowest path
			p.getMu.Lock()
			p.getCond.Wait()
			p.getMu.Unlock()
			tries = 0
		}
	}
	event := p.events[x]
	p.events[x] = nil
	p.free2[x].Store(false)
	p.inUseEvents.Inc()
	event.reset(p.avgEventSize)
	return event
}

func (p *eventPool) back(event *Event) {
	event.stage = eventStagePool
	x := (p.backCounter.Inc() - 1) % int64(p.capacity)
	var tries int
	for {
		// fast path
		if p.free2[x].CAS(false, true) {
			break
		}
		if p.free2[x].CAS(false, true) {
			break
		}
		if p.free2[x].CAS(false, true) {
			break
		}
		tries++
		if tries%maxTries != 0 {
			// slow path
			runtime.Gosched()
		} else {
			// slowest path, sleep instead of cond.Wait because of potential deadlock.
			time.Sleep(5 * time.Millisecond)
			tries = 0
		}
	}
	p.events[x] = event
	p.free1[x].Store(true)
	p.inUseEvents.Dec()
	p.getCond.Broadcast()
}

func (p *eventPool) dump() string {
	out := logger.Cond(len(p.events) == 0, logger.Header("no events"), func() string {
		o := logger.Header("events")
		for i := 0; i < p.capacity; i++ {
			event := p.events[i]
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
