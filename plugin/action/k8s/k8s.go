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
// todo: use informer instead of watch API?
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

func (p *Plugin) Start(config pipeline.AnyConfig) {
	p.config = config.(*Config)

	if p.config.MaxEventSize == 0 {
		p.config.MaxEventSize = int(defaultMaxEventSize)
	}

	startCounter := startCounter.Inc()
	if startCounter == 1 {
		enableGatherer()
	}
}

func (p *Plugin) Stop() {
	startCounter := startCounter.Dec()
	if startCounter == 0 {
		disableGatherer()
	}
}

func (p *Plugin) Reset() {
	p.logBuff = p.logBuff[:0]
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	logFragment := event.JSON.GetStringBytes("log")
	if logFragment == nil {
		logger.Fatalf("wrong docker log format, it doesn't contain log field: %s", event.JSON.String())
		panic("")
	}

	// docker splits long JSON logs by 16kb chunks, so let's join them
	// look ahead 32kb to ensure we won't throw events longer than MaxEventSize
	p.logSize += event.Size
	predictedLen := p.logSize + 32*1024
	isMaxReached := predictedLen > p.config.MaxEventSize
	if logFragment[len(logFragment)-1] != '\n' && !isMaxReached {
		p.logBuff = append(p.logBuff, logFragment...)
		// event size is longer than len(logFragment) because it's quoted

		return pipeline.ActionCollapse
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if isMaxReached {
		logger.Warnf("too long k8s event found, it'll be split, ns=%s pod=%s container=%s consider increase max_event_size, max_event_size=%d, predicted event size=%d", ns, pod, container, p.config.MaxEventSize, predictedLen)
	}

	event.JSON.Set("k8s_namespace", event.JSONPool.NewString(string(ns)))
	event.JSON.Set("k8s_pod", event.JSONPool.NewString(string(pod)))
	event.JSON.Set("k8s_container", event.JSONPool.NewString(string(container)))

	if success {
		if ns != namespace(podMeta.Namespace) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file namespace=%s, meta namespace=%s, event=%s", event.SourceName, ns, podMeta.Namespace, event.JSON.String())
		}
		if pod != podName(podMeta.Name) {
			logger.Panicf("k8s plugin inconsistency: source=%s, file pod=%s, meta pod=%s, x=%s, event=%s", event.SourceName, pod, podMeta.Name, event.JSON.String())
		}

		event.JSON.Set("k8s_node", event.JSONPool.NewString(podMeta.Spec.NodeName))
		for labelName, labelValue := range podMeta.Labels {
			event.JSON.Set("k8s_label_"+labelName, event.JSONPool.NewString(labelValue))
		}
	}

	if len(p.logBuff) > 0 {
		p.logBuff = append(p.logBuff, logFragment...)
		event.JSON.Set("log", event.JSONPool.NewStringBytes(p.logBuff))
		p.logBuff = p.logBuff[:0]
	}
	p.logSize = 0

	return pipeline.ActionPass
}
