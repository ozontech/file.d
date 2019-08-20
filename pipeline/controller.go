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

	parsers     []*Parser
	pipelines   []*pipeline
	splitBuffer *splitBuffer

	inputPlugin Plugin

	done        *sync.WaitGroup
	doneCounter int
	shouldExit  bool

	// some debugging shit
	eventsProcessed *atomic.Int64

	shouldWaitForJob bool
	eventLogEnabled  bool
	eventLog         []string
	eventLogMu       sync.Mutex
}

type ControllerForPlugin interface {
	GetParsers() []*Parser
	GetDone() *sync.WaitGroup
}

func NewController(enableEventLog bool, shouldWaitForJob bool) *Controller {
	procs := runtime.GOMAXPROCS(0)
	parsersCount := procs * 4
	pipelineCount := procs * 4

	logger.Infof("starting new pipeline controller with procs=%d capacity=%d", procs, defaultCapacity)

	controller := &Controller{
		capacity:         defaultCapacity,
		done:             &sync.WaitGroup{},
		eventsProcessed:  atomic.NewInt64(0),
		shouldWaitForJob: shouldWaitForJob,
		eventLogEnabled:  enableEventLog,
		eventLog:         make([]string, 0, 128),
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

	controller.pipelines = pipelines
	controller.parsers = parsers
	controller.splitBuffer = splitBuffer

	return controller
}

func (c *Controller) Start() {
	if c.shouldWaitForJob {
		c.done.Add(1)
	}

	c.inputPlugin.Start()

	for _, pipeline := range c.pipelines {
		pipeline.start(c.splitBuffer)
	}

	go c.reportStats()

}

func (c *Controller) Stop() {
	c.shouldExit = true

	for _, pipeline := range c.pipelines {
		pipeline.stop()
	}

	c.inputPlugin.Stop()
}

func (c *Controller) commit(event *Event) {
	c.eventsProcessed.Inc()

	if c.eventLogEnabled {
		c.eventLogMu.Lock()
		defer c.eventLogMu.Unlock()

		c.eventLog = append(c.eventLog, string(event.raw))
	}
}

func (c *Controller) SetInputPlugin(inputPlugin Plugin) {
	c.inputPlugin = inputPlugin
}

func (c *Controller) splitBufferDone() {
	c.done.Done()
}

func (c *Controller) splitBufferWorks() {
	c.done.Add(1)
	c.doneCounter++
	if c.shouldWaitForJob {
		c.done.Done()
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

func (c *Controller) WaitUntilDone() {
	for {
		c.done.Wait()
		c.doneCounter = 0
		time.Sleep(time.Millisecond * 50)
		if c.doneCounter == 0 {
			break
		}
	}
}

func (c *Controller) GetEventLogLength() int {
	return len(c.eventLog)
}

func (c *Controller) GetEventLogItem(index int) string {
	if index >= len(c.eventLog) {
		logger.Fatalf("Can't find log item with index %d", index)
	}
	return c.eventLog[index]
}

func (c *Controller) GetParsers() []*Parser {
	return c.parsers
}

func (c *Controller) GetDone() *sync.WaitGroup {
	return c.done
}
