package k8s

import (
	"github.com/alecthomas/units"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
)

// Plugin adds k8s meta info to docker logs and also joins split docker logs into one event
// source docker log file name format should be: [pod-name]_[namespace]_[container-name]-[container id].log
// example: /docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log
type Plugin struct {
	config  *Config
	logBuff []byte
	logSize int
}

const (
	defaultMaxEventSize = 1 * units.Megabyte
)

type Config struct {
	MaxEventSize int `json:"max_event_size"`
}

var (
	startCounter atomic.Int32
)

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
		Type:    "k8s",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	if p.config.MaxEventSize == 0 {
		p.config.MaxEventSize = int(defaultMaxEventSize)
	}

	startCounter := startCounter.Inc()
	if startCounter == 1 {
		enableGatherer()
	}

	p.logBuff = append(p.logBuff, '"')
}

func (p *Plugin) Stop() {
	startCounter := startCounter.Dec()
	if startCounter == 0 {
		disableGatherer()
	}
}

func (p *Plugin) Reset() {
	p.logBuff = p.logBuff[:1]
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// don't need to unescape/escape log fields cause concatenation of escaped strings is escaped string
	logFragment := event.Root.Dig("log").AsEscapedString()
	if logFragment == "" {
		logger.Fatalf("wrong docker log format, it doesn't contain log field: %s", event.Root.EncodeToString())
		panic("")
	}

	// docker splits long JSON logs by 16kb chunks, so let's join them
	// look ahead 32kb to ensure we won't throw events longer than MaxEventSize
	p.logSize += event.Size
	predictedLen := p.logSize + 32*1024
	isMaxReached := predictedLen > p.config.MaxEventSize
	logFragmentLen := len(logFragment)
	if logFragment[logFragmentLen-3:logFragmentLen-1] != `\n` && !isMaxReached {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		return pipeline.ActionCollapse
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if isMaxReached {
		logger.Warnf("too long k8s event found, it'll be split, ns=%s pod=%s container=%s consider increase max_event_size, max_event_size=%d, predicted event size=%d", ns, pod, container, p.config.MaxEventSize, predictedLen)
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_namespace").MutateToString(string(ns))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_pod").MutateToString(string(pod))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_container").MutateToString(string(container))

	if success {
		if ns != namespace(podMeta.Namespace) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file namespace=%s, meta namespace=%s, event=%s", event.SourceName, ns, podMeta.Namespace, event.Root.EncodeToString())
		}
		if pod != podName(podMeta.Name) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file pod=%s, meta pod=%s, x=%s, event=%s", event.SourceName, pod, podMeta.Name, event.Root.EncodeToString())
		}

		event.Root.AddFieldNoAlloc(event.Root, "k8s_node").MutateToString(podMeta.Spec.NodeName)
		for labelName, labelValue := range podMeta.Labels {
			//todo: fix copy of strings?
			event.Root.AddFieldNoAlloc(event.Root, "k8s_label_"+labelName).MutateToString(labelValue)
		}
	}

	if len(p.logBuff) > 1 {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		p.logBuff = append(p.logBuff, '"')
		//todo: fix copy of strings?
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToEscapedString(string(p.logBuff))
		p.logBuff = p.logBuff[:1]
	}
	p.logSize = 0

	return pipeline.ActionPass
}
