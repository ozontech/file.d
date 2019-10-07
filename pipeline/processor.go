package pipeline

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

type processorState int

const (
	processorStatePass  processorState = 0
	stateReadSameStream processorState = 1
	stateReadAnyStream  processorState = 2
)

var eventWaitTimeout = time.Second * 30

type ActionResult int

const (
	ActionPass     ActionResult = 0 // pass event to the next action in pipeline
	ActionDiscard  ActionResult = 1 // skip processing of event and commit
	ActionCollapse ActionResult = 2 // request next event from the same stream as current
)

type eventStatus string

const (
	eventStatusReceived   eventStatus = "received"
	eventStatusNotMatched eventStatus = "not_matched"
	eventStatusPassed     eventStatus = "passed"
	eventStatusDiscarded  eventStatus = "discarded"
	eventStatusCollapsed  eventStatus = "collapsed"
)

// processor is worker goroutine which doing pipeline actions
type processor struct {
	id          int
	pipeline    *Pipeline
	actions     []ActionPlugin
	actionsData []*ActionPluginData

	shouldStop bool
	stopWg     *sync.WaitGroup

	state          processorState
	stream         *stream
	event          *Event
	waitingEventID *atomic.Uint64
	metricsValues  []string
}

func NewProcessor(id int, pipeline *Pipeline) *processor {
	processor := &processor{
		id:             id,
		pipeline:       pipeline,
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
	go p.heartbeat()
}

func (p *processor) process(output OutputPlugin) {
	p.state = stateReadAnyStream
	for {
		switch p.state {
		case stateReadAnyStream:
			for {
				if p.stream == nil {
					p.stream = p.pipeline.attachToStream(p)
				}

				p.event = p.stream.instantGet()
				if p.event != nil {
					break
				}

				// if event is nil then stream is over, so let's attach to a new stream
				p.stream = nil
			}
		case stateReadSameStream:
			p.event = p.stream.waitGet(p.waitingEventID)
			if p.event.poolIndex == -1 {
				logger.Errorf("some events are lost, can't read next sequential event from stream %d(%s)", p.stream.sourceId, p.stream.name)
				p.reset()
				p.state = stateReadAnyStream
				p.stream = nil
				continue
			}
			if p.shouldStop {
				return
			}
		default:
			logger.Panicf("wrong state=%d before action in pipeline %s", p.state, p.pipeline.Name)
		}

		p.state = processorStatePass
		for index, action := range p.actions {
			p.state = processorStatePass
			p.countEvent(index, eventStatusReceived)

			if !p.isMatch(index, p.event) {
				p.countEvent(index, eventStatusNotMatched)
				continue
			}

			switch action.Do(p.event) {
			case ActionPass:
				p.countEvent(index, eventStatusPassed)
				p.state = processorStatePass
			case ActionDiscard:
				p.countEvent(index, eventStatusDiscarded)
				// can't notify input here, because previous events may delay and we'll receive offsets corruption
				p.pipeline.commit(p.event, false)
				p.state = stateReadAnyStream
			case ActionCollapse:
				p.countEvent(index, eventStatusCollapsed)
				p.waitingEventID.Swap(p.event.SeqID)
				// can't notify input here, because previous events may delay and we'll receive offsets corruption
				p.pipeline.commit(p.event, false)
				p.state = stateReadSameStream
			}

			if p.state != processorStatePass {
				break
			}
		}

		// if last action returned eventStatusPassed then pass event to output
		if p.state == processorStatePass {
			p.event.stage = eventStageOutput
			p.pipeline.output.Out(p.event)
			p.state = stateReadAnyStream
		}
	}
}

func (p *processor) countEvent(actionIndex int, status eventStatus) {
	p.metricsValues = p.pipeline.countEvent(actionIndex, p.event, status, p.metricsValues)
}

func (p *processor) reset() {
	for _, action := range p.actions {
		action.Reset()
	}
}

func (p *processor) heartbeat() {
	for {
		idA := p.waitingEventID.Load()
		time.Sleep(eventWaitTimeout)
		idB := p.waitingEventID.Load()

		if idA == 0 || idB == 0 || idA != idB {
			continue
		}

		if p.stream.tryUnblockWait(p.waitingEventID) {
			logger.Errorf("event sequence timeout after event id=%d, consider increasing %q", idA, "processors_count")
		}
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
		value := event.Fields.Dig(cond.Field).AsString()
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
		value := event.Fields.Dig(cond.Field).AsString()
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

func (p *processor) dumpState() {
	state := "unknown"
	switch p.state {
	case stateReadAnyStream:
		state = "READING ANY"
	case stateReadSameStream:
		state = fmt.Sprintf("READING STREAM=%d(%s)", p.stream.sourceId, p.stream.name)
	case processorStatePass:
		state = "WORKING"
	}

	logger.Infof("processor id=%d, state=%s", p.id, state)
}

func (p *processor) AddActionPlugin(descr *ActionPluginData) {
	p.actionsData = append(p.actionsData, descr)
	p.actions = append(p.actions, descr.Plugin)
}
