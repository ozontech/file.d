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
	config     *Config
	controller pipeline.ActionController
	fullLog    []byte
	fullSize   int
}

const (
	defaultMaxLogSize = 1 * units.Mebibyte
)

type Config struct {
	MaxLogSize int `json:"max_log_size"`
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

func (p *Plugin) Start(config pipeline.AnyConfig, controller pipeline.ActionController) {
	p.controller = controller
	p.config = config.(*Config)

	if p.config.MaxLogSize == 0 {
		p.config.MaxLogSize = int(defaultMaxLogSize)
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

func (p *Plugin) Do(event *pipeline.Event) {
	chunkedLog := event.JSON.GetStringBytes("log")
	if chunkedLog == nil {
		logger.Fatalf("wrong docker log format, it doesn't contain log field: %s", string(event.JSON.MarshalTo(nil)))
		panic("")
	}

	p.fullSize += event.SourceSize
	isMaxReached := p.fullSize > p.config.MaxLogSize
	//docker splits long JSON logs by 16kb chunks, so let's join them
	if chunkedLog[len(chunkedLog)-1] != '\n' && !isMaxReached {
		p.fullLog = append(p.fullLog, chunkedLog...)
		p.controller.Next()
		return
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if isMaxReached {
		logger.Warnf("too long docker log found, it'll be split, ns=%s pod=%s container=%s consider increase max_log_size, current value=%d", ns, pod, container, p.config.MaxLogSize)
	}

	event.JSON.Set("k8s_namespace", event.JSONPool.NewString(string(ns)))
	event.JSON.Set("k8s_pod", event.JSONPool.NewString(string(pod)))
	event.JSON.Set("k8s_container", event.JSONPool.NewString(string(container)))

	if success {
		event.JSON.Set("k8s_node", event.JSONPool.NewString(podMeta.Spec.NodeName))

		for labelName, labelValue := range podMeta.Labels {
			event.JSON.Set("k8s_label_"+labelName, event.JSONPool.NewString(labelValue))
		}
	}

	if len(p.fullLog) > 0 {
		p.fullLog = append(p.fullLog, chunkedLog...)
		event.JSON.Set("log", event.JSONPool.NewStringBytes(p.fullLog))
		p.fullLog = p.fullLog[:0]
	}
	p.fullSize = 0

	p.controller.Propagate()
}
