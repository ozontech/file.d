package pipeline

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.ozon.ru/sre/filed/logger"
)

type processorState int

const (
	processorStatePass           processorState = 0
	processorStateReadSameStream processorState = 1
	processorStateReadNewStream  processorState = 2
)

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

	state  processorState
	stream *stream

	activeStreams     []*stream
	activeStreamsMu   *sync.Mutex
	activeStreamsCond *sync.Cond
}

func NewProcessor(id int, pipeline *Pipeline) *processor {
	processor := &processor{
		id:       id,
		pipeline: pipeline,
		stopWg:   &sync.WaitGroup{},

		activeStreams:   make([]*stream, 0, 0),
		activeStreamsMu: &sync.Mutex{},
	}

	processor.activeStreamsCond = sync.NewCond(processor.activeStreamsMu)
	return processor
}

func (p *processor) addActiveStream(stream *stream) {
	p.activeStreamsMu.Lock()
	p.activeStreams = append(p.activeStreams, stream)
	stream.index = len(p.activeStreams) - 1
	p.activeStreamsCond.Signal()
	p.activeStreamsMu.Unlock()
}

func (p *processor) removeActiveStream(stream *stream) {
	p.activeStreamsMu.Lock()
	lastIndex := len(p.activeStreams) - 1
	if lastIndex == -1 {
		logger.Panicf("why remove?")
	}
	p.activeStreams[stream.index] = p.activeStreams[lastIndex]
	p.activeStreams[stream.index].index = stream.index
	p.activeStreams = p.activeStreams[:lastIndex]
	p.activeStreamsMu.Unlock()
}

func (p *processor) process(output OutputPlugin) {
	p.state = processorStateReadNewStream
	var event *Event
	for {
		switch p.state {
		case processorStateReadNewStream:
			p.activeStreamsMu.Lock()
			for len(p.activeStreams) == 0 {
				p.activeStreamsCond.Wait()
			}
			p.stream = p.activeStreams[rand.Int()%len(p.activeStreams)]
			p.activeStreamsMu.Unlock()

			event = p.pipeline.getStreamEvent(p.stream, false)
		case processorStateReadSameStream:
			event = p.pipeline.getStreamEvent(p.stream, true)
			if p.shouldStop {
				return
			}
		default:
			logger.Panicf("wrong state=%d before action in pipeline %s", p.state, p.pipeline.name)
		}
		p.state = processorStatePass

		for index, action := range p.actions {
			p.state = processorStatePass
			p.countEvent(index, event, eventStatusReceived)

			if !p.isMatch(index, event) {
				p.countEvent(index, event, eventStatusNotMatched)
				continue
			}

			switch action.Do(event) {
			case ActionPass:
				p.countEvent(index, event, eventStatusPassed)
				p.state = processorStatePass
			case ActionDiscard:
				p.countEvent(index, event, eventStatusDiscarded)
				// can't notify input here, because previous events may delay and we'll receive offsets corruption
				p.pipeline.commit(event, false)
				p.state = processorStateReadNewStream
			case ActionCollapse:
				p.countEvent(index, event, eventStatusCollapsed)
				// can't notify input here, because previous events may delay and we'll receive offsets corruption
				p.pipeline.commit(event, false)
				p.state = processorStateReadSameStream
			}

			if p.state != processorStatePass {
				break
			}
		}

		// if last action returned eventStatusPassed then pass event to output
		if p.state == processorStatePass {
			p.pipeline.output.Out(event)
			p.state = processorStateReadNewStream
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

func (p *processor) start(output OutputPlugin) {
	for i, action := range p.actions {
		action.Start(p.actionsData[i].Config)
	}

	go p.process(output)
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

func (p *processor) dumpState(waitingSourceId SourceId) {
	state := "unknown"
	switch p.state {
	case processorStateReadNewStream:
		state = "READING NEW"
	case processorStateReadSameStream:
		state = fmt.Sprintf("READING SOURCE=%d(%s)", p.stream.sourceId, p.stream.name)
	case processorStatePass:
		state = "WORKING"
	}

	activeStreams := ""
	p.activeStreamsMu.Lock()
	for _, stream := range p.activeStreams {
		activeStreams = activeStreams + fmt.Sprintf("%d(%s)", stream.sourceId, stream.name)
	}
	p.activeStreamsMu.Unlock()
	logger.Infof("processor id=%d, state=%s, active streams=%d: %s", p.id, state, len(p.activeStreams), activeStreams)
}

func (p *processor) AddActionPlugin(descr *ActionPluginData) {
	p.actionsData = append(p.actionsData, descr)
	p.actions = append(p.actions, descr.Plugin)
}
