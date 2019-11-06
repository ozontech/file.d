package pipeline

import (
	"strings"
	"sync"

	"go.uber.org/atomic"
)

type processorState int

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
}

func (p *processor) process(output OutputPlugin) {
	for {
		if p.shouldStop {
			return
		}

		st := p.streamer.reserveStream()
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

		isPassed, readNext := p.doActions(event)

		if isPassed {
			return true, true, event
		}

		if readNext {
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

func (p *processor) doActions(event *Event) (isPassed bool, readSeq bool) {
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
		case ActionDiscard:
			p.countEvent(event, index, eventStatusDiscarded)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.pipeline.commit(event, false)
			return false, false
		case ActionCollapse:
			p.countEvent(event, index, eventStatusCollapse)
			// can't notify input here, because previous events may delay and we'll get offset sequence corruption
			p.pipeline.commit(event, false)
			return false, true
		case ActionHold:
			p.countEvent(event, index, eventStatusHold)
			return false, true
		}
	}

	return true, false
}

func (p *processor) countEvent(event *Event, actionIndex int, status eventStatus) {
	p.metricsValues = p.pipeline.countEvent(event, actionIndex, status, p.metricsValues)
}

func (p *processor) isMatch(index int, event *Event) bool {
	if event.IsTimeoutKind() {
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
	p.actionsData = append(p.actionsData, descr)
	p.actions = append(p.actions, descr.Plugin)
}

func (p *processor) Commit(event *Event) {
	p.pipeline.commit(event, false)
}

func (p *processor) Propagate(event *Event) {
	event.action++
	p.processSequence(event)
}
