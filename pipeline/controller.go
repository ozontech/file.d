package pipeline

import (
	"runtime"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const defaultCapacity = 128

type Controller struct {
	capacity int

	Parsers     []*Parser
	Pipelines   []*pipeline
	SplitBuffer *splitBuffer

	InputPlugin Plugin

	Done       *sync.WaitGroup
	shouldExit bool

	// some debugging shit
	eventsProcessed *atomic.Int64

	shouldWaitForJob bool
	eventLogEnabled  bool
	EventLog         []string
	eventLogMu       sync.Mutex
}

func NewController(enableEventLog bool, shouldWaitForJob bool) *Controller {
	procs := runtime.GOMAXPROCS(0)
	parsersCount := procs * 4
	pipelineCount := procs * 4

	logger.Infof("starting new pipeline controller with procs=%d capacity=%d", procs, defaultCapacity)

	controller := &Controller{
		capacity:         defaultCapacity,
		Done:             &sync.WaitGroup{},
		eventsProcessed:  atomic.NewInt64(0),
		shouldWaitForJob: shouldWaitForJob,
		eventLogEnabled:  enableEventLog,
		EventLog:         make([]string, 0, 128),
		eventLogMu:       sync.Mutex{},
	}

	pipelines := make([]*pipeline, pipelineCount)
	for i := 0; i < pipelineCount; i++ {
		pipelines[i] = NewPipeline(controller)
	}

	splitBuffer := newSplitBuffer(pipelines, controller)

	parsers := make([]*Parser, parsersCount)
	for i := 0; i < parsersCount; i++ {
		parsers[i] = NewParser(splitBuffer)
	}

	controller.Pipelines = pipelines
	controller.Parsers = parsers
	controller.SplitBuffer = splitBuffer

	return controller
}

func (c *Controller) Start() {
	if c.shouldWaitForJob {
		c.Done.Add(1)
	}

	c.InputPlugin.Start()

	for _, pipeline := range c.Pipelines {
		pipeline.start(c.SplitBuffer)
	}

	go c.reportStats()

}

func (c *Controller) Stop() {
	c.shouldExit = true


	for _, pipeline := range c.Pipelines {
		pipeline.stop()
	}

	c.InputPlugin.Stop()
}

func (c *Controller) commit(event *Event) {
	c.eventsProcessed.Inc()

	if c.eventLogEnabled {
		c.eventLogMu.Lock()
		defer c.eventLogMu.Unlock()

		c.EventLog = append(c.EventLog, string(event.raw))
	}
}

func (c *Controller) SetInputPlugin(inputPlugin Plugin) {
	c.InputPlugin = inputPlugin
}

func (c *Controller) splitBufferDone() {
	c.Done.Done()
}

func (c *Controller) splitBufferWorks() {
	c.Done.Add(1)
	if c.shouldWaitForJob {
		c.Done.Done()
		c.shouldWaitForJob = false
	}
}

func (c *Controller) EventsProcessed() int {
	return int(c.eventsProcessed.Load())
}

func (c *Controller) reportStats() {
	lastProcessed := c.eventsProcessed.Load()
	for {
		time.Sleep(InfoReportInterval)
		if c.shouldExit {
			return
		}

		processed := c.eventsProcessed.Load()
		delta := processed - lastProcessed
		rate := float32(delta) / float32(InfoReportInterval) * float32(time.Second)

		logger.Infof("stats info: processed=%d, rate=%.f/sec", delta, rate)

		lastProcessed = delta
	}
}
