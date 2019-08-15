package pipeline

import (
	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/global"
	"go.uber.org/atomic"
	"sync"
)

type event struct {
	index   int
	raw     []byte
	json    *fastjson.Value
	next    *event
	parser  *fastjson.Parser
	isReady bool
}

type splitBuffer struct {
	capacity int

	events      []*event
	eventsMu    sync.Mutex
	eventsCount int

	streams    map[string]*stream
	streamsMu  sync.Mutex
	nextStream chan *stream

	reserves chan bool

	// Some debugging shit
	isTest           bool
	EventLogMu       sync.Mutex
	EventLog         []string
	eventsProcessed  atomic.Int64
	CapacityHits     int
	MaxCapacityUsage int
}

func newSplitBuffer(capacity int, isTest bool) *splitBuffer {
	splitBuffer := &splitBuffer{
		capacity: capacity,

		events:   make([]*event, capacity, capacity),
		eventsMu: sync.Mutex{},

		streams:    make(map[string]*stream),
		streamsMu:  sync.Mutex{},
		nextStream: make(chan *stream, capacity),

		reserves: make(chan bool, capacity),

		isTest:     isTest,
		EventLogMu: sync.Mutex{},
		EventLog:   make([]string, 0, capacity),
	}

	for i := 0; i < capacity; i++ {
		splitBuffer.reserves <- true
		splitBuffer.events[i] = &event{
			parser: &fastjson.Parser{},
		}
	}

	return splitBuffer
}

func (b *splitBuffer) EventsProcessed() int {
	return int(b.eventsProcessed.Load())
}

var streams = []string{
	"1",
	"2",
	"3",
	"4",
	"5",
	"6",
}

// temporary stream selector
var k = 0

func (b *splitBuffer) reserve() *event {
	<-b.reserves

	b.eventsMu.Lock()
	index := b.eventsCount
	event := b.events[index]
	event.index = index
	event.isReady = false
	b.eventsCount++
	b.eventsMu.Unlock()

	if index == b.capacity-1 {
		b.CapacityHits++
	}

	if index > b.MaxCapacityUsage {
		b.MaxCapacityUsage = index + 1
	}

	return event
}

func (b *splitBuffer) push(event *event) {
	if b.isTest {
		b.EventLogMu.Lock()
		b.EventLog = append(b.EventLog, string(event.raw))
		b.EventLogMu.Unlock()
	}

	event.isReady = true

	b.streamsMu.Lock()
	streamId := &streams[k%len(streams)]
	k++
	stream := b.getStream(streamId)
	b.streamsMu.Unlock()

	stream.push(event)
	b.nextStream <- stream
}

func (b *splitBuffer) pop() *event {
	stream := <-b.nextStream

	event := stream.pop()

	return event
}

func (b *splitBuffer) commit(event *event) {
	b.eventsProcessed.Add(1)

	if b.eventsCount == 0 {
		global.Logger.Panic("extra event commit")
	}

	// place event back to pool
	b.eventsMu.Lock()
	b.eventsCount--

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

func (b *splitBuffer) getStream(streamId *string) *stream {
	if b.streams[*streamId] == nil {
		b.addStream(*streamId)
	}
	stream := b.streams[*streamId]
	return stream
}

func (b *splitBuffer) addStream(id string) {
	stream := &stream{
		mu: &sync.Mutex{},
		id: id,
	}

	b.streams[id] = stream
}
