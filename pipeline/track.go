package pipeline

import (
	"strings"

	"gitlab.ozon.ru/sre/filed/logger"
)

const (
	trackStateUnknown        trackState = 0
	trackStatePropagate      trackState = 1
	trackStateReadSameStream trackState = 2
	trackStateReadNewStream  trackState = 3
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

type trackState int

func NewTrack(pipeline *Pipeline) *track {
	return &track{
		pipeline: pipeline,
		streamCh: make(chan *stream, pipeline.capacity),
	}
}

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
			// commit previous event
			t.pipeline.commit(event, false)
			// todo: we may have deadlock here if sequence of events is longer than event pool capacity, event pool must handle overflow
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
				break
			}
		}

		// if last action called Propagate then pass event to output
		if t.state == trackStatePropagate {
			if event.ID%10000 == 0 {
				out, _ := event.Marshal(make([]byte, 0, 0))
				logger.Infof("pipeline %q final event sample: %s", t.pipeline.name, string(out))
			}
			output.Out(event)
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
		value := event.JSON.Get(cond.Field).GetStringBytes()
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
		value := event.JSON.Get(cond.Field).GetStringBytes()
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
	t.state = trackStateReadNewStream
	t.pipeline.commit(event, true)
}

func (t *track) Drop(event *Event) {
	t.state = trackStateReadNewStream
	t.pipeline.commit(event, false)
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
