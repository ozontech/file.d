package actionk8s

import (
	"strings"

	"github.com/valyala/fastjson"
	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"go.uber.org/atomic"
)

// K8SPlugin adds k8s meta info to docker logs
// source docker log file name format should be: [pod-name]_[namespace]_[container-name]-[container id].log
// example: /docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log
type K8SPlugin struct {
	config *Config
}

type Config struct {
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

func factory() (pipeline.AnyPluginPointer, pipeline.AnyConfigPointer) {
	return &K8SPlugin{}, &Config{}
}

func (k *K8SPlugin) Start(config pipeline.AnyConfigPointer, track *pipeline.Track) {
	startCounter := startCounter.Inc()
	if startCounter == 1 {
		enableGatherer()
	}
}

func (k *K8SPlugin) Stop() {
	startCounter := startCounter.Dec()
	if startCounter == 0 {
		disableGatherer()
	}
}

func (k *K8SPlugin) Do(event *pipeline.Event) {
	fullFilename := event.Additional
	ns, pod, container, cid := parseDockerFilename(fullFilename)

	podMeta := getMeta(ns, pod, cid, container)

	// todo: optimize string concatenations
	event.JSON.Set("k8s_pod", fastjson.MustParse(`"`+string(pod)+`"`))
	event.JSON.Set("k8s_ns", fastjson.MustParse(`"`+string(ns)+`"`))
	event.JSON.Set("k8s_container", fastjson.MustParse(`"`+string(container)+`"`))

	if podMeta != nil {
		event.JSON.Set("k8s_node", fastjson.MustParse(`"`+string(podMeta.Spec.NodeName)+`"`))
	}

	metaMu.Lock()
	defer metaMu.Unlock()
	if strings.Contains(string(pod), "advanced-logs-checker") {
		checkerLogs[pod]++
	}
}
