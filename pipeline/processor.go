package pipeline

import (
	"strings"
	"sync"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

type ActionResult int

const (
	// pass event to the next action in a pipeline
	ActionPass ActionResult = 0
	// skip further processing of event and request next event from the same stream and source as current
	// plugin may receive event with eventKindTimeout kind if it takes to long to read next event from same stream
	ActionCollapse ActionResult = 2
	// skip further processing of event and request next event from any stream and source
	ActionDiscard ActionResult = 1
	// hold event in a plugin and request next event from the same stream and source as current.
	// same as ActionCollapse but held event should be manually committed or returned into pipeline.
	// check out Commit()/Propagate() functions in InputPluginController.
	// plugin may receive event with eventKindTimeout kind if it takes to long to read next event from same stream
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

	actions        []ActionPlugin
	actionsData    []*ActionPluginData
	actionsBlocked []bool
	totalBlocked   int

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
}

func (p *processor) process(output OutputPlugin) {
	for {
		if p.shouldStop {
			return
		}

		st := p.streamer.joinStream()
		p.dischargeStream(st)
	}
}

func (p *processor) dischargeStream(st *stream) {
	for {
		event := st.instantGet()
		// if event is nil then stream is over, so let's attach to a new stream
		if event == nil {
			return
		}
		if !p.processSequence(event) {
			return
		}
	}
}

func (p *processor) processSequence(event *Event) bool {
	isSuccess := false
	isPassed := false
	isSuccess, isPassed, event = p.processEvent(event)

	if isPassed {
		event.stage = eventStageOutput
		p.pipeline.output.Out(event)
	}

	return isSuccess
}

func (p *processor) processEvent(event *Event) (isSuccess bool, isPassed bool, e *Event) {
	for {
		stream := event.stream

		if p.doActions(event) {
			return true, true, event
		}

		if p.totalBlocked != 0 {
			action := event.action
			event = stream.blockGet()
			if event.IsTimeoutKind() {
				// pass timeout directly to plugin which requested the sequential event
				event.action = action
			}
			continue
		}

		return true, false, nil
	}
}

func (p *processor) doActions(event *Event) (isPassed bool) {
	l := len(p.actions)
	for index := event.action; index < l; index++ {
		action := p.actions[index]
		event.action = index
		p.countEvent(event, index, eventStatusReceived)

		if !p.isMatch(index, event) {
			p.countEvent(event, index, eventStatusNotMatched)
			continue
		}

		switch action.Do(event) {
		case ActionPass:
			p.countEvent(event, index, eventStatusPassed)
			p.tryUnblockAction(index)
		case ActionDiscard:
			p.countEvent(event, index, eventStatusDiscarded)
			p.tryUnblockAction(index)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.pipeline.finalize(event, false, true)
			return false
		case ActionCollapse:
			p.countEvent(event, index, eventStatusCollapse)
			p.tryBlockAction(index)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.pipeline.finalize(event, false, true)
			return false
		case ActionHold:
			p.countEvent(event, index, eventStatusHold)
			p.tryBlockAction(index)

			p.pipeline.finalize(event, false, false)
			return false
		}
	}

	return true
}

func (p *processor) tryBlockAction(index int) {
	if p.actionsBlocked[index] {
		return
	}
	p.actionsBlocked[index] = true
	p.totalBlocked++

	if p.totalBlocked > len(p.actions) {
		logger.Panicf("blocked actions too big")
	}
}

func (p *processor) tryUnblockAction(index int) {
	if !p.actionsBlocked[index] {
		return
	}
	p.actionsBlocked[index] = false
	p.totalBlocked--

	if p.totalBlocked < 0 {
		logger.Panicf("blocked action count less than zero")
	}
}

func (p *processor) countEvent(event *Event, actionIndex int, status eventStatus) {
	p.metricsValues = p.pipeline.countEvent(event, actionIndex, status, p.metricsValues)
}

func (p *processor) isMatch(index int, event *Event) bool {
	if event.IsTimeoutKind() {
		return true
	}

	if p.actionsBlocked[index] {
		return true
	}

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

func (p *processor) AddActionPlugin(descr *ActionPluginData) {
	p.actions = append(p.actions, descr.Plugin)
	p.actionsData = append(p.actionsData, descr)
	p.actionsBlocked = append(p.actionsBlocked, false)
}

func (p *processor) Commit(event *Event) {
	p.pipeline.finalize(event, false, true)
}

func (p *processor) Propagate(event *Event) {
	event.action++
	p.processSequence(event)
}
