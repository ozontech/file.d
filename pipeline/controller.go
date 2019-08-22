package pipeline

import (
	"math/rand"
	"runtime"
	"sync"
	"time"

	"gitlab.ozon.ru/sre/filed/logger"
	"go.uber.org/atomic"
)

const statsInfoReportInterval = time.Second * 5
const defaultCapacity = 128

type Controller interface {
	GetHeads() []*Head
	GetDone() *sync.WaitGroup
}

type SplitController struct {
	capacity   int
	done       *sync.WaitGroup
	doneHelper bool

	heads       []*Head
	pipelines   []*pipeline
	splitBuffer *splitBuffer

	inputPlugin   *PluginWithConfig
	actionPlugins []*PluginWithConfig

	shouldExit bool

	shouldWaitForJob bool
	eventLogEnabled  bool
	eventLog         []string
	eventLogMu       sync.Mutex

	// some debugging shit
	eventsProcessed *atomic.Int64
}

func NewController(enableEventLog bool) *SplitController {
	procs := runtime.GOMAXPROCS(0)
	pipelineCount := procs * 4
	headsCount := procs * 4

	logger.Infof("starting new pipeline controller with procs=%d capacity=%d", procs, defaultCapacity)

	controller := &SplitController{
		capacity: defaultCapacity,
		done:     &sync.WaitGroup{},

		actionPlugins: make([]*PluginWithConfig, 0, 4),

		shouldWaitForJob: true,
		eventLogEnabled:  enableEventLog,
		eventLog:         make([]string, 0, 128),
		eventLogMu:       sync.Mutex{},
		eventsProcessed:  atomic.NewInt64(0),
	}

	splitBuffer := newSplitBuffer(controller)

	heads := make([]*Head, headsCount)
	for i := 0; i < headsCount; i++ {
		heads[i] = NewHead(splitBuffer)
	}

	pipelines := make([]*pipeline, pipelineCount)
	for i := 0; i < pipelineCount; i++ {
		pipelines[i] = NewPipeline(controller)
	}

	controller.splitBuffer = splitBuffer
	controller.heads = heads
	controller.pipelines = pipelines

	return controller
}

func (c *SplitController) Start() {
	c.done.Add(1)

	c.inputPlugin.Instance.Start(c.inputPlugin.Config, c)

	for _, pluginInfo := range c.actionPlugins {
		pluginInfo.Instance.Start(pluginInfo.Config, c)
	}

	for _, pipeline := range c.pipelines {
		pipeline.start()
	}

	go c.reportStats()

}

func (c *SplitController) Stop() {
	c.shouldExit = true

	for _, pipeline := range c.pipelines {
		pipeline.stop()
	}

	c.inputPlugin.Instance.Stop()
}

func (c *SplitController) commit(event *Event) {
	c.splitBuffer.commit(event)

	c.eventsProcessed.Inc()
	c.doneHelper = true

	if c.eventLogEnabled {
		c.eventLogMu.Lock()
		defer c.eventLogMu.Unlock()

		c.eventLog = append(c.eventLog, string(event.raw))
	}
}

func (c *SplitController) SetInputPlugin(plugin *PluginWithConfig) {
	c.inputPlugin = plugin
}

func (c *SplitController) AddActionPlugin(plugin *PluginWithConfig) {
	c.actionPlugins = append(c.actionPlugins, plugin)
}

func (c *SplitController) EventsProcessed() int {
	return int(c.eventsProcessed.Load())
}

func (c *SplitController) attachStream(stream *stream) {
	// Assign random pipeline for stream to have uniform event distribution
	pipeline := c.pipelines[rand.Int()%len(c.pipelines)]
	pipeline.addStream(stream)
}

func (c *SplitController) reportStats() {
	lastProcessed := c.eventsProcessed.Load()
	for {
		time.Sleep(statsInfoReportInterval)
		if c.shouldExit {
			return
		}

		processed := c.eventsProcessed.Load()
		delta := processed - lastProcessed
		rate := float32(delta) / float32(statsInfoReportInterval) * float32(time.Second)

		logger.Infof("stats for last %d seconds: processed=%d, rate=%.f/sec", statsInfoReportInterval/time.Second, delta, rate)

		lastProcessed = processed
	}
}

func (c *SplitController) ResetDone() {
	c.shouldWaitForJob = true
	c.done.Add(1)
}

func (c *SplitController) WaitUntilDone() {
	for {
		c.done.Wait()
		// fs events may have delay, so wait for them
		time.Sleep(time.Millisecond * 100)

		if !c.doneHelper {
			break
		}
		c.doneHelper = false
	}
}

func (c *SplitController) GetEventLogLength() int {
	return len(c.eventLog)
}

func (c *SplitController) GetEventLogItem(index int) string {
	if index >= len(c.eventLog) {
		logger.Fatalf("Can't find log item with index %d", index)
	}
	return c.eventLog[index]
}

func (c *SplitController) GetHeads() []*Head {
	return c.heads
}

func (c *SplitController) GetDone() *sync.WaitGroup {
	return c.done
}
