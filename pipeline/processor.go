package pipeline

import (
	"github.com/ozontech/file.d/logger"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type ActionResult int

const (
	// ActionPass pass event to the next action in a pipeline
	ActionPass ActionResult = 0
	// ActionCollapse skip further processing of event and request next event from the same stream and source as current
	// plugin may receive event with EventKindTimeout if it takes to long to read next event from same stream
	ActionCollapse ActionResult = 2
	// ActionDiscard skip further processing of event and request next event from any stream and source
	ActionDiscard ActionResult = 1
	// ActionHold hold event in a plugin and request next event from the same stream and source as current.
	// same as ActionCollapse but held event should be manually committed or returned into pipeline.
	// check out Commit()/Propagate() functions in InputPluginController.
	// plugin may receive event with EventKindTimeout if it takes to long to read next event from same stream.
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

func allEventStatuses() []eventStatus {
	return []eventStatus{
		eventStatusReceived,
		eventStatusNotMatched,
		eventStatusPassed,
		eventStatusDiscarded,
		eventStatusCollapse,
		eventStatusHold,
	}
}

// processor is a goroutine which doing pipeline actions
type processor struct {
	id            int
	streamer      *streamer
	metricsHolder *metricsHolder
	output        OutputPlugin
	finalize      finalizeFn

	activeCounter *atomic.Int32

	actions          []ActionPlugin
	actionInfos      []*ActionPluginStaticInfo
	busyActions      []bool
	busyActionsTotal int
	actionWatcher    *actionWatcher
	recoverFromPanic func()

	metricsValues []string

	useExpFeatures bool
}

func newProcessor(
	id int,
	metricsHolder *metricsHolder,
	activeCounter *atomic.Int32,
	output OutputPlugin,
	streamer *streamer,
	finalizeFn finalizeFn,
	useExpFeatures bool,
) *processor {
	processor := &processor{
		id:            id,
		streamer:      streamer,
		metricsHolder: metricsHolder,
		output:        output,
		finalize:      finalizeFn,

		activeCounter: activeCounter,
		actionWatcher: newActionWatcher(id),

		metricsValues: make([]string, 0),

		useExpFeatures: useExpFeatures,
	}

	return processor
}

func (p *processor) start(params PluginDefaultParams, log *zap.SugaredLogger) {
	for i, action := range p.actions {
		actionInfo := p.actionInfos[i]
		action.Start(actionInfo.PluginStaticInfo.Config, &ActionPluginParams{
			PluginDefaultParams: params,
			Controller:          p,
			Logger:              log.Named("action").Named(actionInfo.Type),
		})
	}

	go p.process()
}

func (p *processor) process() {
	for {
		st := p.streamer.joinStream()
		if st == nil {
			return
		}

		p.activeCounter.Inc()
		p.dischargeStream(st)
		p.activeCounter.Dec()
	}
}

func (p *processor) dischargeStream(st *stream) {
	for {
		event := st.instantGet()
		// if event is nil then stream is over, so let's attach to a new stream.
		if event == nil {
			return
		}
		if !p.processSequence(event) {
			return
		}
	}
}

func (p *processor) processSequence(event *Event) bool {
	isPassed := false
	isPassed, event = p.processEvent(event)

	if isPassed {
		if event.IsUnlockKind() {
			return false
		}

		event.stage = eventStageOutput
		p.output.Out(event)
	}

	return true
}

func (p *processor) processEvent(event *Event) (isPassed bool, e *Event) {
	for {
		if event.IsUnlockKind() {
			return true, event
		}
		stream := event.stream

		passed, lastAction := p.doActions(event)
		if passed {
			return true, event
		}
		event = nil // this event can be returned to the pool

		// no busy actions, so return.
		if p.busyActionsTotal == 0 {
			return false, nil
		}

		// there is busy action, waiting for next sequential event.
		event = stream.blockGet()
		if event.IsTimeoutKind() {
			// pass timeout directly to plugin which requested next sequential event.
			event.action = lastAction
		}
	}
}

func (p *processor) doActions(event *Event) (isPassed bool, lastAction int) {
	l := len(p.actions)
	for index := event.action; index < l; index++ {
		action := p.actions[index]
		event.action = index
		p.countEvent(event, index, eventStatusReceived)

		if !p.busyActions[index] && !event.IsTimeoutKind() {
			if !p.isMatch(index, event) {
				p.countEvent(event, index, eventStatusNotMatched)
				continue
			}
		}

		p.actionWatcher.setEventBefore(index, event)

		switch action.Do(event) {
		case ActionPass:
			p.countEvent(event, index, eventStatusPassed)
			p.tryResetBusy(index)
			p.actionWatcher.setEventAfter(index, event, eventStatusPassed)
		case ActionDiscard:
			p.countEvent(event, index, eventStatusDiscarded)
			p.tryResetBusy(index)
			// can't notify input here, because previous events may delay, and we'll get offset sequence corruption.
			p.finalize(event, false, true)
			p.actionWatcher.setEventAfter(index, event, eventStatusDiscarded)
			return false, index
		case ActionCollapse:
			p.countEvent(event, index, eventStatusCollapse)
			p.tryMarkBusy(index)
			// can't notify input here, because previous events may delay, and we'll get offset sequence corruption.
			p.finalize(event, false, true)
			p.actionWatcher.setEventAfter(index, event, eventStatusCollapse)
			return false, index
		case ActionHold:
			p.countEvent(event, index, eventStatusHold)
			p.tryMarkBusy(index)

			p.finalize(event, false, false)
			p.actionWatcher.setEventAfter(index, event, eventStatusHold)
			return false, index
		}
	}

	// return the last action index as the event has passed all the actions
	return true, l - 1
}

func (p *processor) tryMarkBusy(index int) {
	if p.busyActions[index] {
		return
	}
	p.busyActions[index] = true
	p.busyActionsTotal++

	if p.busyActionsTotal > len(p.actions) {
		logger.Panicf("blocked actions too big")
	}
}

func (p *processor) tryResetBusy(index int) {
	if !p.busyActions[index] {
		return
	}
	p.busyActions[index] = false
	p.busyActionsTotal--

	if p.busyActionsTotal < 0 {
		logger.Panicf("blocked action count less than zero")
	}
}

func (p *processor) countEvent(event *Event, actionIndex int, status eventStatus) {
	if event.IsTimeoutKind() {
		return
	}
	p.metricsValues = p.metricsHolder.count(event, actionIndex, status, p.metricsValues)
}

func (p *processor) isMatch(index int, event *Event) bool {
	info := p.actionInfos[index]

	if p.useExpFeatures && info.DoIfChecker != nil {
		return info.DoIfChecker.Check(event.Root, p.id)
	}

	conds := info.MatchConditions
	mode := info.MatchMode
	match := false

	if mode == MatchModeOr || mode == MatchModeOrPrefix {
		match = p.isMatchOr(conds, event, mode == MatchModeOrPrefix)
	} else {
		match = p.isMatchAnd(conds, event, mode == MatchModeAndPrefix)
	}

	if info.MatchInvert {
		match = !match
	}

	return match
}

func (p *processor) isMatchOr(conds MatchConditions, event *Event, byPrefix bool) bool {
	for _, cond := range conds {
		node := event.Root.Dig(cond.Field...)
		if node == nil {
			continue
		}
		value := node.AsString()
		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
			if match {
				return true
			}
		}

		match = cond.valueExists(value, byPrefix)
		if match {
			return true
		}
	}

	return false
}

func (p *processor) isMatchAnd(conds MatchConditions, event *Event, byPrefix bool) bool {
	for _, cond := range conds {
		node := event.Root.Dig(cond.Field...)
		if node == nil {
			return false
		}
		value := node.AsString()

		match := false
		if cond.Regexp != nil {
			match = cond.Regexp.MatchString(value)
			if !match {
				return false
			}
		}

		match = cond.valueExists(value, byPrefix)
		if !match {
			return false
		}
	}

	return true
}

func (p *processor) stop() {
	p.streamer.unblockProcessor()

	for _, action := range p.actions {
		action.Stop()
	}
}

func (p *processor) AddActionPlugin(info *ActionPluginInfo) {
	p.actions = append(p.actions, info.Plugin.(ActionPlugin))
	p.actionInfos = append(p.actionInfos, info.ActionPluginStaticInfo)
	p.busyActions = append(p.busyActions, false)
}

// Propagate flushes an event after ActionHold.
func (p *processor) Propagate(event *Event) {
	event.action++
	nextActionIdx := event.action
	p.tryResetBusy(nextActionIdx - 1)
	p.processSequence(event)
}

func (p *processor) RecoverFromPanic() {
	p.recoverFromPanic()
}
