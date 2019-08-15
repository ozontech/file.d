package pipeline

import (
	"sync"
)

type stream struct {
	id    string
	index int
	mu    *sync.Mutex

	first *event
	last  *event

	length int
}

func (s *stream) push(event *event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++
	event.next = nil

	if (s.last == nil) {
		s.last = event
		s.first = event
		return
	}

	s.last.next = event
	s.last = event
	return
}

func (s *stream) pop() (*event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--
	if (s.first == nil) {
		panic("stream is empty, why pop?")
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
