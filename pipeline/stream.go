package pipeline

import (
	"sync"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

// stream is a queue of events
// all events in the queue are from one source and also has same field value (eg json field "stream" in docker logs)
type stream struct {
	readyIndex int
	name       StreamName
	sourceId   SourceId
	pipeline   *Pipeline

	mu   *sync.Mutex
	cond *sync.Cond

	isDetaching  bool
	isAttached   bool
	getOffset    int64
	commitOffset int64

	first *Event
	last  *Event
}

func newStream(name StreamName, sourceId SourceId, pipeline *Pipeline) *stream {
	stream := stream{
		readyIndex: -1,
		name:       name,
		sourceId:   sourceId,
		pipeline:   pipeline,
		mu:         &sync.Mutex{},
	}
	stream.cond = sync.NewCond(stream.mu)

	return &stream
}

func (s *stream) detach() {
	if s.isDetaching {
		logger.Panicf("why detach? stream is already detaching")
	}
	if !s.isAttached {
		logger.Panicf("why detach? stream isn't attached")
	}
	s.isDetaching = true
	s.tryDropProcessor()
}

func (s *stream) commit(event *Event) {
	s.mu.Lock()
	// we need to get max here because discarded events with
	// bigger offsets can be committed faster than events with lower
	// offsets which are going through output
	if event.Offset > s.commitOffset {
		s.commitOffset = event.Offset
	}
	if s.isDetaching {
		s.tryDropProcessor()
	}
	s.mu.Unlock()
}

func (s *stream) tryDropProcessor() {
	if s.getOffset != s.commitOffset {
		return
	}

	s.isAttached = false
	s.isDetaching = false

	if s.first != nil {
		s.pipeline.addReadyStream(s)
	}
}

func (s *stream) attach(processor *processor) {
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
	s.isDetaching = false
	s.mu.Unlock()
}

func (s *stream) put(event *Event) {
	s.mu.Lock()
	event.stream = s
	event.stage = eventStageStream
	if s.first == nil {
		s.last = event
		s.first = event
		if !s.isAttached {
			s.pipeline.addReadyStream(s)
		}
		s.cond.Signal()
	} else {
		s.last.next = event
		s.last = event
	}
	s.mu.Unlock()
}

func (s *stream) waitGet(waitingEventId *atomic.Uint64) *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why wait get? stream isn't attached")
	}
	for s.first == nil {
		s.cond.Wait()
	}
	waitingEventId.Swap(0)
	event := s.get()
	// it was timeout, not real event
	if event.poolIndex == -1 {
		s.detach()
	}

	s.mu.Unlock()

	return event
}

func (s *stream) instantGet() *Event {
	s.mu.Lock()
	if !s.isAttached {
		logger.Panicf("why instant get? stream isn't attached")
	}
	if s.first == nil {
		s.detach()
		s.mu.Unlock()

		return nil
	}
	event := s.get()
	s.mu.Unlock()

	return event
}

func (s *stream) tryUnblockWait(waitingEventId *atomic.Uint64) bool {
	if s == nil {
		return false
	}

	s.mu.Lock()
	id := waitingEventId.Load()
	if id == 0 {
		s.mu.Unlock()
		return false
	}

	if s.first != nil {
		logger.Panicf("why stream isn't empty? get offset=%d, commit offset=%d", s.getOffset, s.commitOffset)
	}

	if s.getOffset != s.commitOffset {
		logger.Panicf("why offsets are different? get offset=%d, commit offset=%d", s.getOffset, s.commitOffset)
	}

	event := &Event{poolIndex: -1, Offset: s.commitOffset}
	s.last = event
	s.first = event
	s.cond.Signal()
	s.mu.Unlock()

	return true
}

func (s *stream) get() *Event {
	if s.isDetaching {
		logger.Panicf("why get while detaching?")
	}

	result := s.first
	if s.first == s.last {
		s.first = nil
		s.last = nil
	} else {
		s.first = s.first.next
	}

	result.stage = eventStageProcessor
	// stream was reset somewhere
	if result.Offset < s.getOffset {
		s.commitOffset = 0
	}
	s.getOffset = result.Offset
	return result
}
