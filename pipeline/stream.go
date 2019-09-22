package pipeline

import (
	"sync"
)

// stream is a queue of events
// all events in the queue are from one source and also has same field value (eg json field "stream" in docker logs)
type stream struct {
	index     int
	name      StreamName
	sourceId  SourceId
	processor *processor
	mu        *sync.Mutex
	cond      *sync.Cond

	first *Event
	last  *Event
}

func newStream(name StreamName, sourceId SourceId, processor *processor) *stream {
	stream := stream{
		name:      name,
		sourceId:  sourceId,
		processor: processor,
		mu:        &sync.Mutex{},
	}
	stream.cond = sync.NewCond(stream.mu)

	return &stream
}

func (s *stream) put(event *Event) {
	s.mu.Lock()
	event.processorID = s.processor.id
	if s.first == nil {
		s.last = event
		s.first = event
		s.processor.addActiveStream(s)
		s.cond.Signal()
	} else {
		s.last.next = event
		s.last = event
	}

	s.mu.Unlock()
}

func (s *stream) waitGet() *Event {
	s.mu.Lock()
	for s.first == nil {
		s.cond.Wait()
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) instantGet() *Event {
	s.mu.Lock()
	if s.first == nil {
		s.mu.Unlock()
		return nil
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) get() *Event {
	if s.first == s.last {
		result := s.first
		s.first = nil
		s.last = nil
		s.processor.removeActiveStream(s)

		return result
	}

	result := s.first
	s.first = s.first.next

	return result
}
