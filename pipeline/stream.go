package pipeline

import (
	"sync"
)

type stream struct {
	pipeline *pipeline
	sourceId uint64
	name     string
	mu       *sync.Mutex

	first *Event
	last  *Event

	length int
}

func (s *stream) push(event *Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++
	event.next = nil

	if s.last == nil {
		s.last = event
		s.first = event

		s.pipeline.nextStream <- s
		return
	}

	s.last.next = event
	s.last = event

	s.pipeline.nextStream <- s
	return
}

func (s *stream) tryPop() *Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--
	if s.first == nil {
		return nil
	}

	if s.first == s.last {
		result := s.first
		s.first = nil
		s.last = nil

		return result
	}

	result := s.first
	s.first = s.first.next

	return result
}
