package pipeline

import (
	"strings"

	"gitlab.ozon.ru/sre/filed/logger"
)

const (
	trackStateUnknown        trackState = 0
	trackStateReadSameStream trackState = 1
	trackStateCommit         trackState = 2
	trackStatePropagate      trackState = 3
	trackStateReadNewStream  trackState = 4
)

// track is worker goroutine which doing pipeline actions
type track struct {
	pipeline     *Pipeline
	descriptions []*ActionPluginDescription
	actions      []ActionPlugin

	shouldStop bool

	streamCh chan *stream

	state trackState
}

func NewTrack(pipeline *Pipeline) *track {
	return &track{
		pipeline: pipeline,
		streamCh: make(chan *stream, pipeline.capacity),
	}
}

type trackState int

func (t *track) process(output OutputPlugin) {
	t.state = trackStateReadNewStream
	var event *Event
	var stream *stream
	for {
		switch t.state {
		case trackStateReadNewStream:
			for {
				stream = <-t.streamCh
				if t.shouldStop {
					return
				}
				event = stream.instantGet()
				if event != nil {
					break
				}
			}
		case trackStateReadSameStream:
			event = stream.waitGet()
			if t.shouldStop {
				return
			}
		default:
			logger.Panicf("wrong state=%d before action in pipeline %s", t.state, t.pipeline.name)
		}

		t.state = trackStatePropagate
		for i, action := range t.actions {
			if t.match(i, event) {
				t.state = trackStateUnknown
				action.Do(event)
			} else {
				continue
			}

			if t.state == trackStateUnknown {
				logger.Panicf("action plugin %s haven't call one of next/commit/propagate for pipeline %s", t.descriptions[i].Type, t.pipeline.name)
			}

			if t.state == trackStateReadNewStream {
				logger.Panicf("wrong state=%d after action in pipeline %s", t.state, t.pipeline.name)
			}

			if t.state == trackStateCommit {
				t.state = trackStateReadNewStream
			}
		}

		if t.state == trackStatePropagate {
			output.Dump(event)
			t.state = trackStateReadNewStream
		}
	}
}

func (t *track) match(index int, event *Event) bool {
	descr := t.descriptions[index]
	conds := descr.MatchConditions
	mode := descr.MatchMode

	if mode == ModeOr {
		return t.matchOr(conds, event)
	} else {
		return t.matchAnd(conds, event)
	}
}

func (t *track) matchOr(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Value.Get(cond.Field).GetStringBytes()
		if value == nil {
			continue
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.Match(value)
		} else {
			match = strings.Trim(ByteToString(value), " ") == cond.Value
		}

		if match {
			return true
		}
	}

	return false
}

func (t *track) matchAnd(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Value.Get(cond.Field).GetStringBytes()
		if value == nil {
			return false
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.Match(value)
		} else {
			match = strings.Trim(ByteToString(value), " ") == cond.Value
		}

		if !match {
			return false
		}
	}

	return true
}

func (t *track) Next() {
	t.state = trackStateReadSameStream

}
func (t *track) Commit(event *Event) {
	t.state = trackStateCommit
	t.pipeline.Commit(event)
}

func (t *track) Propagate() {
	t.state = trackStatePropagate
}

func (t *track) start(output OutputPlugin) {
	t.shouldStop = false
	for i, action := range t.actions {
		action.Start(t.descriptions[i].Config, t)
	}

	go t.process(output)
}

func (t *track) stop() {
	for _, action := range t.actions {
		action.Stop()
	}

	t.shouldStop = true
	//unblock goroutine
	t.streamCh <- nil
}

func (t *track) AddActionPlugin(descr *ActionPluginDescription) {
	t.descriptions = append(t.descriptions, descr)
	t.actions = append(t.actions, descr.Plugin)
}
