package pipeline

import (
	"sync"

	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/logger"
)

type Event struct {
	index int
	next  *Event

	Value      *fastjson.Value
	Offset     int64
	SourceId   SourceId
	Stream     StreamName
	Additional string

	parser *fastjson.Parser

	// some debugging shit
	raw []byte
}

type eventPool struct {
	capacity    int
	eventsCount int

	pool []*Event
	mu   *sync.Mutex
	cond *sync.Cond
}

func newEventPool(capacity int) *eventPool {

	eventPool := &eventPool{
		capacity: capacity,
		pool:     make([]*Event, capacity, capacity),
		mu:       &sync.Mutex{},
	}

	eventPool.cond = sync.NewCond(eventPool.mu)

	for i := 0; i < capacity; i++ {
		eventPool.pool[i] = &Event{
			parser: &fastjson.Parser{},
			index:  i,
		}
	}

	return eventPool
}

func (b *eventPool) get() (*Event, int) {
	b.mu.Lock()

	for b.eventsCount >= b.capacity-1 {
		b.cond.Wait()
	}

	index := b.eventsCount
	event := b.pool[index]
	b.eventsCount++

	b.mu.Unlock()

	return event, index
}

func (b *eventPool) back(event *Event) int {
	b.mu.Lock()

	b.eventsCount--

	currentIndex := event.index
	lastIndex := b.eventsCount

	if lastIndex == -1 {
		logger.Panic("extra event commit")
	}

	last := b.pool[lastIndex]

	// exchange event with last one to place in back to pool
	b.pool[currentIndex] = last
	b.pool[lastIndex] = event

	event.index = lastIndex
	last.index = currentIndex

	b.cond.Signal()

	b.mu.Unlock()

	return lastIndex
}
