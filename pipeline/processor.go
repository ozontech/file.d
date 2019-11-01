package pipeline

import (
	"strings"
	"sync"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

type processorState int

const (
	statePass           processorState = 0
	stateReadSameStream processorState = 1
	stateReadAnyStream  processorState = 2
)

type ActionResult int

const (
	// pass event to the next action in a pipeline
	ActionPass ActionResult = 0
	// skip further processing of event and request next event from the same stream and source as current
	ActionCollapse ActionResult = 2
	// skip further processing of event and request next event from any stream and source
	ActionDiscard ActionResult = 1
	// hold event in a plugin  and request next event from the same stream and source as current.
	// held event should be manually committed or returned into pipeline.
	// check out Commit()/Propagate() functions in InputPluginController.
	ActionHold ActionResult = 3
)

type eventStatus string

const (
	eventStatusReceived   eventStatus = "received"
	eventStatusNotMatched eventStatus = "not_matched"
	eventStatusPassed     eventStatus = "passed"
	eventStatusDiscarded  eventStatus = "discarded"
	eventStatusCollapse   eventStatus = "collapsed"
	eventStatusHold       eventStatus = "held"
)

// processor is worker goroutine which doing pipeline actions
type processor struct {
	id       int
	pipeline *Pipeline
	streamer *streamer

	actions     []ActionPlugin
	actionsData []*ActionPluginData

	shouldStop bool
	stopWg     *sync.WaitGroup

	heartbeatCh    chan *stream
	waitingEventID *atomic.Uint64
	metricsValues  []string
}

func NewProcessor(id int, pipeline *Pipeline, streamer *streamer) *processor {
	processor := &processor{
		id:       id,
		pipeline: pipeline,
		streamer: streamer,

		stopWg:         &sync.WaitGroup{},
		waitingEventID: &atomic.Uint64{},
		metricsValues:  make([]string, 0, 0),
	}

	return processor
}

func (p *processor) start(output OutputPlugin, params *ActionPluginParams) {
	for i, action := range p.actions {
		action.Start(p.actionsData[i].Config, params)
	}

	go p.process(output)
	//go p.heartbeat()
}

func (p *processor) readAnyStream(st *stream) (*Event, *stream) {
	for {
		if st == nil {
			st = p.streamer.reserve()
		}

		event := st.instantGet()
		if event != nil {
			return event, st
		}

		// if event is nil then stream is over, so let's attach to a new stream
		st = nil
	}
}

func (p *processor) process(output OutputPlugin) {
	var event *Event
	var st *stream
	for {
		if p.shouldStop {
			return
		}

		event, st = p.readAnyStream(st)
		if !p.processEvent(event, st) {
			st = nil
		}
	}
}

func (p *processor) processEvent(event *Event, st *stream) bool {
	for {
		if p.shouldStop {
			return true
		}

		isPassed := true
		for index, action := range p.actions {
			isPassed = true
			p.countEvent(event, index, eventStatusReceived)

			if !p.isMatch(index, event) {
				p.countEvent(event, index, eventStatusNotMatched)
				continue
			}

			switch action.Do(event) {
			case ActionPass:
				p.countEvent(event, index, eventStatusPassed)
			case ActionDiscard:
				p.countEvent(event, index, eventStatusDiscarded)
				// can't notify input here, because previous events may delay and we'll get offset sequence corruption
				p.pipeline.commit(event, false)
				return true
			case ActionCollapse:
				p.countEvent(event, index, eventStatusCollapse)
				// can't notify input here, because previous events may delay and we'll get offset sequence corruption
				p.pipeline.commit(event, false)
				isPassed = false
			case ActionHold:
				p.countEvent(event, index, eventStatusHold)
				isPassed = false
			}

			if isPassed {
				break
			}
		}

		// if actions are passed then send event to output
		if isPassed {
			event.stage = eventStageOutput
			p.pipeline.output.Out(event)
			return true
		}

		offset := event.Offset
		event = st.blockGet()
		if event.index == -1 {
			logger.Errorf("can't read next sequential event from stream %s:%d(%s), offset=%d", st.sourceName, st.sourceId, st.name, offset)
			p.reset()
			return false
		}
	}
}
func (p *processor) countEvent(event *Event, actionIndex int, status eventStatus) {
	p.metricsValues = p.pipeline.countEvent(event, actionIndex, status, p.metricsValues)
}

func (p *processor) reset() {
	for _, action := range p.actions {
		action.Reset()
	}
}

func (p *processor) isMatch(index int, event *Event) bool {
	descr := p.actionsData[index]
	conds := descr.MatchConditions
	mode := descr.MatchMode

	if mode == ModeOr {
		return p.isMatchOr(conds, event)
	} else {
		return p.isMatchAnd(conds, event)
	}
}

func (p *processor) isMatchOr(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Root.Dig(cond.Field).AsString()
		if value == "" {
			continue
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
		} else {
			match = strings.Trim(value, " ") == cond.Value
		}

		if match {
			return true
		}
	}

	return false
}

func (p *processor) isMatchAnd(conds MatchConditions, event *Event) bool {
	for _, cond := range conds {
		value := event.Root.Dig(cond.Field).AsString()
		if value == "" {
			return false
		}

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
		} else {
			match = strings.Trim(value, " ") == cond.Value
		}

		if !match {
			return false
		}
	}

	return true
}

func (p *processor) stop() {
	p.shouldStop = true

	for _, action := range p.actions {
		action.Stop()
	}
}

//func (p *processor) dumpState() {
//	state := "unknown"
//	switch p.state {
//	case stateReadAnyStream:
//		state = "READING ANY"
//	case stateReadSameStream:
//		state = fmt.Sprintf("READING STREAM=%d(%s)", p.stream.sourceId, p.stream.name)
//	case statePass:
//		state = "WORKING"
//	}
//
//	logger.Infof("processor id=%d, state=%s", p.id, state)
//}

func (p *processor) AddActionPlugin(descr *ActionPluginData) {
	p.actionsData = append(p.actionsData, descr)
	p.actions = append(p.actions, descr.Plugin)
}
