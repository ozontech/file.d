package pipeline

import (
	"math/rand"
	"sync"
	"time"

	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/logger"
)

const InfoReportInterval = time.Second * 2

type splitBuffer struct {
	controller *Controller

	events      []*Event
	eventsMu    sync.Mutex
	eventsCount int

	done      *sync.WaitGroup
	resetDone bool

	pipelines  []*pipeline
	streams    map[string]map[string]*stream
	streamsMu  sync.Mutex
	nextStream chan *stream

	reserves chan bool

	CapacityHits     int
	MaxCapacityUsage int
}

func newSplitBuffer(pipelines []*pipeline, controller *Controller) *splitBuffer {
	capacity := controller.capacity

	splitBuffer := &splitBuffer{
		controller: controller,

		events:   make([]*Event, capacity, capacity),
		eventsMu: sync.Mutex{},

		pipelines:  pipelines,
		streams:    make(map[string]map[string]*stream),
		streamsMu:  sync.Mutex{},
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
		b.controller.splitBufferWorks()
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
		b.controller.splitBufferDone()
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
	b.streamsMu.Lock()
	defer b.streamsMu.Unlock()

	s := b.streams[event.Stream]
	if s == nil {
		b.streams[event.Stream] = make(map[string]*stream)
		s = b.streams[event.Stream]
	}

	subStream := s[event.SubStream]
	if subStream == nil {
		subStream = b.addStream(event)

	}
	return subStream
}

func (b *splitBuffer) addStream(event *Event) *stream {
	stream := &stream{
		mu:        &sync.Mutex{},
		stream:    event.Stream,
		subStream: event.SubStream,
	}

	b.streams[event.Stream][event.SubStream] = stream

	// Assign random pipeline for stream
	pipeline := b.pipelines[rand.Int()%len(b.pipelines)]
	pipeline.addStream(stream)

	return stream
}
