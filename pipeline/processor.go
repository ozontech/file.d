package pipeline

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
}

func NewProcessor(id int, pipeline *Pipeline) *processor {
	processor := &processor{
		id:             id,
		pipeline:       pipeline,
		stopWg:         &sync.WaitGroup{},
		waitingEventID: &atomic.Uint64{},
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
			logger.Panicf("wrong state=%d before action in pipeline %s", p.state, p.pipeline.name)
		}

		p.state = processorStatePass
		for index, action := range p.actions {
			p.state = processorStatePass
			p.countEvent(index, p.event, eventStatusReceived)

			if !p.isMatch(index, p.event) {
				p.countEvent(index, p.event, eventStatusNotMatched)
				continue
			}

			switch action.Do(p.event) {
			case ActionPass:
				p.countEvent(index, p.event, eventStatusPassed)
				p.state = processorStatePass
			case ActionDiscard:
				p.countEvent(index, p.event, eventStatusDiscarded)
				// can't notify input here, because previous events may delay and we'll receive offsets corruption
				p.pipeline.commit(p.event, false)
				p.state = stateReadAnyStream
			case ActionCollapse:
				p.countEvent(index, p.event, eventStatusCollapsed)
				p.waitingEventID.Swap(p.event.ID)
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

func (p *processor) isMatchAnd(conds MatchConditions, event *Event) bool {
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

func (p *processor) stop() {
	p.shouldStop = true

	for _, action := range p.actions {
		action.Stop()
	}
}

func (p *processor) nextMetricsGeneration(metricsGen string) {
	for _, data := range p.actionsData {
		if data.MetricName == "" {
			continue
		}

		counter := prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace:   "filed",
			Subsystem:   "pipeline_" + p.pipeline.name,
			Name:        data.MetricName + "_events_total",
			Help:        fmt.Sprintf("how many events processed by pipeline %q and action %q", p.pipeline.name, data.T),
			ConstLabels: map[string]string{"gen": data.ID + "_" + metricsGen},
		}, append([]string{"status"}, data.LabelNames...))

		prev := data.counterPrev
		data.counterPrev = data.counter
		data.counter = counter

		p.pipeline.registry.MustRegister(counter)
		if prev != nil {
			p.pipeline.registry.Unregister(prev)
		}
	}
}

func (p *processor) countEvent(actionIndex int, event *Event, eventStatus eventStatus) {
	data := p.actionsData[actionIndex]
	if data.MetricName == "" {
		return
	}

	data.labelValues = data.labelValues[:0]
	data.labelValues = append(data.labelValues, string(eventStatus))

	for _, field := range data.LabelNames {
		value := event.JSON.GetStringBytes(field)
		valueStr := defaultFieldValue
		if value != nil {
			valueStr = string(value)
		}

		data.labelValues = append(data.labelValues, valueStr)
	}

	data.counter.WithLabelValues(data.labelValues...).Inc()
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
