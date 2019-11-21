package pipeline

import (
	"fmt"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
)

type streamer struct {
	streams map[SourceID]map[StreamName]*stream
	mu      *sync.RWMutex

	charged     []*stream
	chargedMu   *sync.Mutex
	chargedCond *sync.Cond

	blocked   []*stream
	blockedMu *sync.Mutex
}

func newStreamer() *streamer {
	streamer := &streamer{
		streams: make(map[SourceID]map[StreamName]*stream),
		mu:      &sync.RWMutex{},
		charged: make([]*stream, 0, 0),

		chargedMu: &sync.Mutex{},
		blockedMu: &sync.Mutex{},
	}
	streamer.chargedCond = sync.NewCond(streamer.chargedMu)

	return streamer
}

func (s *streamer) start() {
	go s.heartbeat()
}

func (s *streamer) putEvent(sourceID SourceID, streamName StreamName, event *Event) {
	s.getStream(sourceID, streamName).put(event)
}

func (s *streamer) getStream(sourceID SourceID, streamName StreamName) *stream {
	// fast path, stream have been already created
	s.mu.RLock()
	st, has := s.streams[sourceID][streamName]
	s.mu.RUnlock()
	if has {
		return st
	}

	// slow path, create new stream
	s.mu.Lock()
	defer s.mu.Unlock()
	st, has = s.streams[sourceID][streamName]
	if has {
		return st
	}

	_, has = s.streams[sourceID]
	if !has {
		s.streams[sourceID] = make(map[StreamName]*stream)
	}

	st = newStream(streamName, sourceID, s)
	s.streams[sourceID][streamName] = st

	return st
}

func (s *streamer) joinStream() *stream {
	s.chargedMu.Lock()
	for len(s.charged) == 0 {
		s.chargedCond.Wait()
	}
	stream := s.charged[0]
	stream.attach()
	s.resetCharged(stream)
	s.chargedMu.Unlock()

	return stream
}

func (s *streamer) makeBlocked(stream *stream) {
	s.blockedMu.Lock()
	stream.blockIndex = len(s.blocked)
	s.blocked = append(s.blocked, stream)
	s.blockedMu.Unlock()
}

func (s *streamer) resetBlocked(stream *stream) {
	s.blockedMu.Lock()
	if stream.blockIndex == -1 {
		logger.Panicf("why remove? stream isn't blocked")
	}

	lastIndex := len(s.blocked) - 1
	if lastIndex == -1 {
		logger.Panicf("why remove? stream isn't in block list")
	}
	index := stream.blockIndex
	s.blocked[index] = s.blocked[lastIndex]
	s.blocked[index].blockIndex = index
	s.blocked = s.blocked[:lastIndex]

	stream.blockIndex = -1
	s.blockedMu.Unlock()
}

func (s *streamer) makeCharged(stream *stream) {
	s.chargedMu.Lock()
	stream.chargeIndex = len(s.charged)
	s.charged = append(s.charged, stream)
	s.chargedCond.Signal()
	s.chargedMu.Unlock()
}

func (s *streamer) resetCharged(stream *stream) {
	if stream.chargeIndex == -1 {
		logger.Panicf("why remove? stream isn't ready")
	}

	lastIndex := len(s.charged) - 1
	if lastIndex == -1 {
		logger.Panicf("why remove? stream isn't in ready list")
	}
	index := stream.chargeIndex
	s.charged[index] = s.charged[lastIndex]
	s.charged[index].chargeIndex = index
	s.charged = s.charged[:lastIndex]

	stream.chargeIndex = -1
}

func (s *streamer) heartbeat() {
	streams := make([]*stream, 0, 0)
	for {
		time.Sleep(time.Millisecond * 200)
		streams = streams[:0]

		s.blockedMu.Lock()
		streams = append(streams, s.blocked...)
		s.blockedMu.Unlock()

		for _, stream := range streams {
			stream.tryUnblock()
		}
	}
}

func (s *streamer) dump() string {
	s.mu.Lock()

	out := logger.Cond(len(s.streams) == 0, logger.Header("no streams"), func() string {
		o := logger.Header("streams")
		for _, s := range s.streams {
			for _, stream := range s {
				state := "| UNATTACHED |"
				if stream.isAttached {
					state = "|  ATTACHED  |"
				}
				if stream.isDetaching {
					state = "| DETACHING  |"
				}

				o += fmt.Sprintf("%d(%s) state=%s, away event id=%d, commit event id=%d, len=%d\n", stream.sourceID, stream.name, state, stream.awaySeq, stream.commitSeq, stream.len)
			}
		}

		return o
	})

	out += logger.Cond(len(s.charged) == 0, logger.Header("charged streams empty"), func() string {
		o := logger.Header("charged streams")
		for _, s := range s.charged {
			o += fmt.Sprintf("%d(%s)\n", s.sourceID, s.name)
		}

		return o
	})

	out += logger.Cond(len(s.blocked) == 0, logger.Header("blocked streams empty"), func() string {
		o := logger.Header("blocked streams")
		for _, s := range s.blocked {
			o += fmt.Sprintf("%d(%s)\n", s.sourceID, s.name)
		}

		return o
	})

	s.mu.Unlock()

	return out
}
