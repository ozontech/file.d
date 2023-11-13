package pipeline

import (
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/logger"
)

// stream is a queue of events
// events fall into stream based on rules defined by input plugin
// streams are used to allow event joins and other operations which needs sequential event input
// e.g. events from same file will be in same stream for "file" input plugin
type stream struct {
	blockIndex int
	currentSeq atomic.Uint64

	onReadyAttach func(readyStream *stream)
	blockTimeout  time.Duration

	isAttached atomic.Bool
	isReady    atomic.Bool

	queueLen atomic.Int64
	queue    chan *Event
}

func newStream(onReadyAttach func(readyStream *stream), blockTimeout time.Duration) *stream {
	return &stream{
		onReadyAttach: onReadyAttach,
		blockTimeout:  blockTimeout,
		queue:         make(chan *Event, 256),
	}
}

func (s *stream) put(event *Event) uint64 {
	seqID := s.currentSeq.Add(1)
	event.stream = s
	event.stage = eventStageStream
	event.SeqID = seqID
	s.queue <- event
	s.queueLen.Add(1)
	if !s.isReady.Swap(true) {
		s.onReadyAttach(s)
	}
	return seqID
}

func (s *stream) detach() {
	if !s.isAttached.Load() {
		logger.Panicf("why detach? stream isn't attached")
	}

	s.isAttached.Store(false)
	s.isReady.Store(false)

	if s.queueLen.Load() != 0 {
		//s.onReadyAttach(s)
	}
}

func (s *stream) attach() {
	if s.isAttached.Swap(true) {
		// already attached
		return
	}

	if v := s.queueLen.Load(); v == 0 {
		//logger.Panicf("why attach? stream is empty")
	}
	s.isAttached.Store(true)
}

func (s *stream) blockGet() *Event {
	if !s.isAttached.Load() {
		logger.Panicf("why wait get? stream isn't attached")
	}

	timeout := time.NewTimer(s.blockTimeout)
	select {
	case event := <-s.queue:
		s.queueLen.Add(-1)
		//s.awaySeq = event.SeqID
		event.stage = eventStageProcessor
		return event
	case <-timeout.C:
		return s.unblockEvent()
	}
}

func (s *stream) instantGet() *Event {
	if !s.isAttached.Load() {
		logger.Panicf("why instant get? stream isn't attached")
	}

	select {
	case event := <-s.queue:
		s.queueLen.Add(-1)
		//s.awaySeq = event.SeqID
		event.stage = eventStageProcessor
		return event
	default:
		s.detach()
		return nil
	}
}

func (s *stream) unblockEvent() *Event {
	return newTimeoutEvent(s)
}
