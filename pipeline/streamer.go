package pipeline

import (
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
)

type streamer struct {
	streams map[SourceID]map[StreamName]*stream
	mu      *sync.RWMutex

	ready     []*stream
	readyMu   *sync.Mutex
	readyCond *sync.Cond

	blocked   []*stream
	blockedMu *sync.Mutex
}

func newStreamer() *streamer {
	streamer := &streamer{
		streams: make(map[SourceID]map[StreamName]*stream),
		mu:      &sync.RWMutex{},
		ready:   make([]*stream, 0, 0),

		readyMu:   &sync.Mutex{},
		blockedMu: &sync.Mutex{},
	}
	streamer.readyCond = sync.NewCond(streamer.readyMu)

	return streamer
}

func (s *streamer) start() {
	go s.heartbeat()
}

func (s *streamer) putEvent(event *Event) {
	s.getStream(event.StreamName, event.SourceID, event.SourceName).put(event)
}

func (s *streamer) getStream(streamName StreamName, sourceId SourceID, sourceName string) *stream {
	// fast path, stream have been already created
	s.mu.RLock()
	st, has := s.streams[sourceId][streamName]
	s.mu.RUnlock()
	if has {
		return st
	}

	// slow path, create new stream
	s.mu.Lock()
	defer s.mu.Unlock()
	st, has = s.streams[sourceId][streamName]
	if has {
		return st
	}

	_, has = s.streams[sourceId]
	if !has {
		s.streams[sourceId] = make(map[StreamName]*stream)
	}

	st = newStream(streamName, sourceId, sourceName, s)
	s.streams[sourceId][streamName] = st

	return st
}

func (s *streamer) reserve() *stream {
	s.readyMu.Lock()
	for len(s.ready) == 0 {
		s.readyCond.Wait()
	}
	stream := s.ready[0]
	stream.attach()
	s.resetReady(stream)
	s.readyMu.Unlock()

	return stream
}

func (s *streamer) makeBlocked(stream *stream) {
	s.blockedMu.Lock()
	stream.blockIndex = len(s.blocked)
	s.blocked = append(s.blocked, stream)
	s.blockedMu.Unlock()
}

func (s *streamer) resetBlocked(stream *stream) {
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
}

func (s *streamer) makeReady(stream *stream) {
	s.readyMu.Lock()
	stream.readyIndex = len(s.ready)
	s.ready = append(s.ready, stream)
	s.readyCond.Signal()
	s.readyMu.Unlock()
}

func (s *streamer) resetReady(stream *stream) {
	if stream.readyIndex == -1 {
		logger.Panicf("why remove? stream isn't ready")
	}

	lastIndex := len(s.ready) - 1
	if lastIndex == -1 {
		logger.Panicf("why remove? stream isn't in ready list")
	}
	index := stream.readyIndex
	s.ready[index] = s.ready[lastIndex]
	s.ready[index].readyIndex = index
	s.ready = s.ready[:lastIndex]

	stream.readyIndex = -1
}

func (s *streamer) heartbeat() {
	for {
		time.Sleep(time.Millisecond * 200)
		s.blockedMu.Lock()
		for _, stream := range s.blocked {
			if stream.tryUnblock() {
				logger.Errorf(`event sequence timeout consider increasing "processors_count" parameter`)
			}
		}
		s.blockedMu.Unlock()
	}
}
