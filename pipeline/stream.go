package pipeline

import (
	"sync"
	"time"

	"github.com/ozontech/file.d/logger"
)

// stream is a queue of events
// events fall into stream based on rules defined by input plugin
// streams are used to allow event joins and other operations which needs sequential event input
// e.g. events from same file will be in same stream for "file" input plugin
// todo: remove dependency on streamer
type stream struct {
	blockIndex  int
	len         int
	currentSeq  uint64
	commitSeq   uint64
	awaySeq     uint64

	name       StreamName
	sourceID   SourceID
	streamer   *streamer
	blockTime  time.Time

	mu   *sync.Mutex
	cond *sync.Cond

	isDetaching bool
	isAttached  bool

	first *Event
	last  *Event
}

func newStream(name StreamName, sourceID SourceID, streamer *streamer) *stream {
	stream := stream{
		name:     name,
		sourceID: sourceID,
		streamer: streamer,
		mu:       &sync.Mutex{},
	}
	stream.cond = sync.NewCond(stream.mu)

	return &stream
}

func (s *stream) leave() {
	if s.isDetaching {
		logger.Panicf("why detach? stream is already detaching")
	}
	if !s.isAttached {
		logger.Panicf("why detach? stream isn't attached")
	}
	s.isDetaching = true
	s.tryDetach()
}

func (s *stream) commit(event *Event) {
	s.mu.Lock()
	// maxID is needed here because discarded events with bigger offsets may be
	// committed faster than events with lower offsets which are goes through output
	if event.SeqID < s.commitSeq {
		s.mu.Unlock()
		return
	}
	s.commitSeq = event.SeqID

	if s.isDetaching {
		s.tryDetach()
	}
	s.mu.Unlock()
}

func (s *stream) tryDetach() {
	if s.awaySeq != s.commitSeq {
		return
	}

	s.isAttached = false
	s.isDetaching = false

	if s.first != nil {
		s.streamer.makeCharged(s)
	}
}

func (s *stream) attach() {
	s.mu.Lock()
	if s.isAttached {
		logger.Panicf("why attach? processor is already attached")
	}
	if s.isDetaching {
		logger.Panicf("why attach? processor is detaching")
	}
	if s.first == nil {
		logger.Panicf("why attach? stream is empty")
	}

	s.isAttached = true
	s.mu.Unlock()
}

func (s *stream) put(event *Event) uint64 {
	s.mu.Lock()
	s.len++
	s.currentSeq++
	seqID := s.currentSeq
	event.stream = s
	event.stage = eventStageStream
	event.SeqID = seqID
	if s.first == nil {
		s.last = event
		s.first = event
		if !s.isAttached {
			s.streamer.makeCharged(s)
		}
		s.cond.Signal()
	} else {
		s.last.next = event
		s.last = event
	}
	s.mu.Unlock()

	return seqID
}

func (s *stream) blockGet() *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why wait get? stream isn't attached")
	}
	for s.first == nil {
		s.blockTime = time.Now()
		s.streamer.makeBlocked(s)
		s.cond.Wait()
		s.streamer.resetBlocked(s)
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) instantGet() *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why instant get? stream isn't attached")
	}
	if s.first == nil {
		s.leave()
		s.mu.Unlock()

		return nil
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) tryUnblock() bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	if time.Since(s.blockTime) < s.streamer.eventTimeout {
		s.mu.Unlock()
		return false
	}

	// is it lock after put signal?
	if s.first != nil {
		s.mu.Unlock()
		return false
	}

	if s.awaySeq != s.commitSeq {
		logger.Panicf("why events are different? away event id=%d, commit event id=%d", s.awaySeq, s.commitSeq)
	}

	timeoutEvent := newTimoutEvent(s)
	s.last = timeoutEvent
	s.first = timeoutEvent

	s.cond.Signal()
	s.mu.Unlock()

	return true
}

func (s *stream) get() *Event {
	if s.isDetaching {
		logger.Panicf("why get while detaching?")
	}

	event := s.first
	if s.first == s.last {
		s.first = nil
		s.last = nil
	} else {
		s.first = s.first.next
	}

	if event != nil {
		s.awaySeq = event.SeqID
		event.stage = eventStageProcessor
		s.len--
	}

	return event
}
