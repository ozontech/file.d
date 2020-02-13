package k8s

import (
	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

/*{ introduction
Adds kubernetes meta information into events collected from docker log files. Also joins split docker logs into one event.

Source docker log file name should be in format:<br> `[pod-name]_[namespace]_[container-name]-[container-id].log` 

E.g. `my_pod-1566485760-trtrq_my-namespace_my-container-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

Information which plugin adds: 
* `k8s_node` – node name where pod is running
* `k8s_pod` – pod name
* `k8s_namespace` – pod namespace name
* `k8s_container` – pod container name
* `k8s_label_*` – pod labels

}*/
type Plugin struct {
	config  *Config
	logger  *zap.SugaredLogger
	logBuff []byte
	logSize int
}

const (
	predictionLookahead = 128 * 1024
)

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> Docker splits long logs by 16kb chunks. Plugin joins them back, but if event will be longer than this value in bytes it will be split after all.
	//> > Because of optimization it's not strict rule. Events may be split even if they won't gonna exceed the limit.
	MaxEventSize int `json:"max_event_size" default:"1000000"` //*

	//> @3@4@5@6
	//>
	//> If set defines which pod labels to add to the event, others will be ignored.
	LabelsWhitelist  []string `json:"labels_whitelist"` //*
	LabelsWhitelist_ map[string]bool

	//> @3@4@5@6
	//>
	//> Skip retrieving k8s meta information using kubernetes API and add only `k8s_node` field.
	OnlyNode bool `json:"only_node" default:"false"` //*
}

var (
	startCounter atomic.Int32
)

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "k8s",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger

	startCounter := startCounter.Inc()
	p.config.LabelsWhitelist_ = cfg.ListToMap(p.config.LabelsWhitelist)

	if startCounter == 1 {
		enableGatherer(p.logger)
	}

	p.logBuff = append(p.logBuff, '"')
}

func (p *Plugin) Stop() {
	startCounter := startCounter.Dec()
	if startCounter == 0 {
		disableGatherer()
	}
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	// todo: do same logic as in join plugin here to send not full logs
	if event.IsTimeoutKind() {
		p.logger.Errorf("can't read next sequential event for k8s pod stream")
		p.logBuff = p.logBuff[:1]
		return pipeline.ActionDiscard
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_node").MutateToString(node)
	if p.config.OnlyNode {
		return pipeline.ActionPass
	}
	// don't need to unescape/escape log fields cause concatenation of escaped strings is escaped string
	logFragment := event.Root.Dig("log").AsEscapedString()
	if logFragment == "" {
		p.logger.Fatalf("wrong docker log format, it doesn't contain log field: %s", event.Root.EncodeToString())
		panic("")
	}

	// docker splits long logs by 16kb chunks, so let's join them
	// look ahead to ensure we won't throw events longer than MaxEventSize
	// lookahead value is much more than 16Kb because json may be escaped
	p.logSize += event.Size
	predictedLen := p.logSize + predictionLookahead
	isMaxReached := predictedLen > p.config.MaxEventSize
	logFragmentLen := len(logFragment)
	if logFragment[logFragmentLen-3:logFragmentLen-1] != `\n` && !isMaxReached {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		return pipeline.ActionCollapse
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if isMaxReached {
		p.logger.Warnf("too long k8s event found, it'll be split, ns=%s pod=%s container=%s consider increase max_event_size, max_event_size=%d, predicted event size=%d", ns, pod, container, p.config.MaxEventSize, predictedLen)
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_namespace").MutateToString(string(ns))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_pod").MutateToString(string(pod))
	event.Root.AddFieldNoAlloc(event.Root, "k8s_container").MutateToString(string(container))

	if success {
		if ns != namespace(podMeta.Namespace) {
			p.logger.Panicf("k8s plugin inconsistency: source=%s, file namespace=%s, meta namespace=%s, event=%s", event.SourceName, ns, podMeta.Namespace, event.Root.EncodeToString())
		}
		if pod != podName(podMeta.Name) {
			p.logger.Panicf("k8s plugin inconsistency: source=%s, file pod=%s, meta pod=%s, x=%s, event=%s", event.SourceName, pod, podMeta.Name, event.Root.EncodeToString())
		}

		for labelName, labelValue := range podMeta.Labels {
			if len(p.config.LabelsWhitelist_) != 0 {
				_, has := p.config.LabelsWhitelist_[labelName]

				if !has {
					continue
				}
			}

			l := len(event.Buf)
			event.Buf = append(event.Buf, "k8s_label_"...)
			event.Buf = append(event.Buf, labelName...)
			event.Root.AddFieldNoAlloc(event.Root, pipeline.ByteToStringUnsafe(event.Buf[l:])).MutateToString(labelValue)
		}
	}

	if len(p.logBuff) > 1 {
		p.logBuff = append(p.logBuff, logFragment[1:logFragmentLen-1]...)
		p.logBuff = append(p.logBuff, '"')

		l := len(event.Buf)
		event.Buf = append(event.Buf, p.logBuff...)
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToEscapedString(pipeline.ByteToStringUnsafe(event.Buf[l:]))

		p.logBuff = p.logBuff[:1]
	}
	p.logSize = 0

	return pipeline.ActionPass
}
