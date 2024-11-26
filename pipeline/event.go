package pipeline

import (
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/atomic"
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
	// Size in bytes of the raw event before any action plugin.
	Size int

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

func (e *Event) reset() {
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

func (e *Event) Encode(outBuf []byte) ([]byte, int) {
	l := len(outBuf)
	outBuf = e.Root.Encode(outBuf)
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

	stopped          *atomic.Bool
	runHeartbeatOnce *sync.Once
	slowWaiters      *atomic.Int64
	wakeupInterval   time.Duration
}

func (p *eventPool) stop() {
	p.stopped.Store(true)
}

func (p *eventPool) waiters() int64 {
	return p.slowWaiters.Load()
}

func newEventPool(capacity, avgEventSize int) *eventPool {
	eventPool := &eventPool{
		avgEventSize:     avgEventSize,
		capacity:         capacity,
		getMu:            &sync.Mutex{},
		backCounter:      *atomic.NewInt64(int64(capacity)),
		runHeartbeatOnce: &sync.Once{},
		stopped:          atomic.NewBool(false),
		slowWaiters:      atomic.NewInt64(0),
		wakeupInterval:   time.Second * 5,
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

func (p *eventPool) get(size int) *Event {
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
			p.runHeartbeatOnce.Do(func() {
				// Run heartbeat to periodically wake up goroutines that are waiting.
				go p.wakeupWaiters()
			})

			// slowest path
			p.slowWaiters.Inc()
			p.getMu.Lock()
			p.getCond.Wait()
			p.getMu.Unlock()
			p.slowWaiters.Dec()
			tries = 0
		}
	}
	event := p.events[x]
	p.events[x] = nil
	p.free2[x].Store(false)
	p.inUseEvents.Inc()
	event.stage = eventStageInput
	event.Size = size
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
	p.resetEvent(event)
	p.events[x] = event
	p.free1[x].Store(true)
	p.inUseEvents.Dec()
	p.getCond.Broadcast()
}

func (p *eventPool) wakeupWaiters() {
	for {
		if p.stopped.Load() {
			return
		}

		time.Sleep(p.wakeupInterval)
		waiters := p.slowWaiters.Load()
		eventsAvailable := p.inUseEvents.Load() < int64(p.capacity)
		if waiters > 0 && eventsAvailable {
			// There are events in the pool, wake up waiting goroutines.
			p.getCond.Broadcast()
		}
	}
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

func (p *eventPool) resetEvent(e *Event) {
	if e.Size > p.avgEventSize {
		e.Root.ReleaseBufMem()
	}

	if cap(e.Buf) > 4096 {
		e.Buf = make([]byte, 0, 1024)
	}

	if e.Root.PoolSize() > DefaultJSONNodePoolSize*4 {
		e.Root.ReleasePoolMem()
	}

	e.reset()
}

func (p *eventPool) inUse() int64 {
	return p.inUseEvents.Load()
}

const syncPools = 33

type lowMemoryEventPool struct {
	capacity int

	// pools contains sync.Pool instances of various capacities.
	//
	//	pools[0] [0,  1)
	//	pools[1] [1,  2)
	//	pools[2] [2,  4)
	//	pools[3] [4,  8)
	//	pools[4] [8,  16)
	//	pools[5] [16, 32)
	//	pools[6] [32, 64)
	//	...
	//	pools[30] [512MiB, 1GiB)
	//	pools[31] [1GiB,   2BiG)
	//	pools[32] [2GiB,   4GiB)
	pools       [syncPools]*sync.Pool
	inUseEvents *atomic.Int64
	getCond     *sync.Cond

	stopped          *atomic.Bool
	runHeartbeatOnce *sync.Once
	slowWaiters      *atomic.Int64
	wakeupInterval   time.Duration
}

func newLowMemoryEventPool(capacity int) *lowMemoryEventPool {
	pools := [syncPools]*sync.Pool{}
	for i := 0; i < syncPools; i++ {
		pools[i] = &sync.Pool{
			New: func() any {
				return newEvent()
			},
		}
	}
	return &lowMemoryEventPool{
		capacity:         capacity,
		pools:            pools,
		inUseEvents:      &atomic.Int64{},
		getCond:          sync.NewCond(&sync.Mutex{}),
		stopped:          &atomic.Bool{},
		runHeartbeatOnce: &sync.Once{},
		slowWaiters:      &atomic.Int64{},
		wakeupInterval:   time.Second * 5,
	}
}

func (p *lowMemoryEventPool) get(size int) *Event {
	if size < 0 {
		panic(fmt.Errorf("BUG: negative event size: %d", size))
	}

	index := poolIndex(size)
	getPool := p.pools[index]

again:
	inUse := int(p.inUseEvents.Inc())
	// Fast path: we're not over the capacity.
	if inUse <= p.capacity {
		e := getPool.Get().(*Event)
		e.Size = size
		return e
	}

	// Slow path: wait until we fit in the capacity.
	p.inUseEvents.Dec()

	// Run heartbeat to periodically wake up goroutines that are waiting.
	p.runHeartbeatOnce.Do(func() {
		go p.wakeupWaiters()
	})

	// Wait until we fit in the capacity.
	p.slowWaiters.Inc()
	p.getCond.L.Lock()
	if !p.eventsAvailable() {
		p.getCond.Wait()
	}
	p.getCond.L.Unlock()
	p.slowWaiters.Dec()
	goto again
}

func (p *lowMemoryEventPool) back(event *Event) {
	index := poolIndex(event.Size)
	backPool := p.pools[index]

	event.reset()
	backPool.Put(event)
	p.inUseEvents.Dec()
	p.getCond.Broadcast()
}

func poolIndex(size int) int {
	return bits.Len(uint(size))
}

func (p *lowMemoryEventPool) dump() string {
	return fmt.Sprintf("in use events: %d of %d; waiters: %d", p.inUseEvents.Load(), p.capacity, p.slowWaiters.Load())
}

func (p *lowMemoryEventPool) inUse() int64 {
	inUse := p.inUseEvents.Load()
	// Make sure we don't overflow the capacity.
	// It's possible in case when we call inUseEvents.Inc() when the pool is full.
	inUse = min(inUse, int64(p.capacity))
	return inUse
}

func (p *lowMemoryEventPool) waiters() int64 {
	return p.slowWaiters.Load()
}

func (p *lowMemoryEventPool) stop() {
	p.stopped.Store(true)
}

func (p *lowMemoryEventPool) wakeupWaiters() {
	for {
		if p.stopped.Load() {
			return
		}

		time.Sleep(p.wakeupInterval)
		waiters := p.slowWaiters.Load()
		eventsAvailable := p.eventsAvailable()
		if waiters > 0 && !eventsAvailable {
			// There are events in the pool, wake up waiting goroutines.
			p.getCond.Broadcast()
		}
	}
}

func (p *lowMemoryEventPool) eventsAvailable() bool {
	return int(p.inUseEvents.Load()) < p.capacity
}
