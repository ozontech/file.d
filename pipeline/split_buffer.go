package pipeline

import (
	"sync"

	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/logger"
)

type splitBuffer struct {
	controller *SplitController

	events      []*Event
	eventsMu    sync.Mutex
	eventsCount int

	sources    map[uint64]map[string]*stream
	sourcesMu  sync.Mutex
	nextStream chan *stream

	reserves chan bool

	CapacityHits     int
	MaxCapacityUsage int
}

func newSplitBuffer(controller *SplitController) *splitBuffer {
	capacity := controller.capacity

	splitBuffer := &splitBuffer{
		controller: controller,

		events:   make([]*Event, capacity, capacity),
		eventsMu: sync.Mutex{},

		sources:    make(map[uint64]map[string]*stream),
		sourcesMu:  sync.Mutex{},
		nextStream: make(chan *stream, capacity),

		reserves: make(chan bool, capacity),
	}

	for i := 0; i < capacity; i++ {
		splitBuffer.reserves <- true
		splitBuffer.events[i] = &Event{
			parser: &fastjson.Parser{},
		}
	}

	return splitBuffer
}

func (b *splitBuffer) Reserve() *Event {
	<-b.reserves
	index, event := b.reserve()

	if index == b.controller.capacity-1 {
		b.CapacityHits++
	}
	if index > b.MaxCapacityUsage {
		b.MaxCapacityUsage = index + 1
	}

	return event
}

func (b *splitBuffer) reserve() (int, *Event) {
	b.eventsMu.Lock()
	defer b.eventsMu.Unlock()

	if b.eventsCount == 0 {
		b.controller.done.Add(1)
		if b.controller.shouldWaitForJob {
			b.controller.done.Done()
			b.controller.shouldWaitForJob = false
		}
	}

	index := b.eventsCount
	event := b.events[index]
	event.index = index
	b.eventsCount++

	return index, event
}

func (b *splitBuffer) Push(event *Event) {
	stream := b.getStream(event)
	stream.push(event)
}

func (b *splitBuffer) commit(event *Event) {
	if b.eventsCount == 0 {
		logger.Panic("extra event commit")
	}

	event.input.Commit(event)

	b.eventsMu.Lock()
	b.eventsCount--

	if b.eventsCount == 0 {
		b.controller.done.Done()
	}

	// place event back to pool
	current := event.index
	last := b.eventsCount
	tmp := b.events[current]

	b.events[current] = b.events[b.eventsCount]
	b.events[current].index = current

	b.events[last] = tmp
	b.events[last].index = last

	b.eventsMu.Unlock()

	b.reserves <- true
}

func (b *splitBuffer) getStream(event *Event) *stream {
	b.sourcesMu.Lock()
	defer b.sourcesMu.Unlock()

	if b.sources[event.SourceId] == nil {
		b.sources[event.SourceId] = make(map[string]*stream)
	}

	stream := b.sources[event.SourceId][event.Stream]
	if stream == nil {
		stream = b.instantiateStream(event)
		b.sources[event.SourceId][event.Stream] = stream
	}

	return stream
}

func (b *splitBuffer) instantiateStream(event *Event) *stream {
	stream := &stream{
		mu:       &sync.Mutex{},
		sourceId: event.SourceId,
		name:     event.Stream,
	}

	b.controller.attachStream(stream)

	return stream
}
