package k8s

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

type MultilineAction struct {
	config        *Config
	logger        *zap.SugaredLogger
	params        *pipeline.ActionPluginParams
	maxEventSize  int
	eventBuf      []byte
	eventSize     int
	skipNextEvent bool
}

const (
	predictionLookahead = 128 * 1024
)

func (p *MultilineAction) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger
	p.params = params
	p.maxEventSize = params.PipelineSettings.MaxEventSize
	p.config = config.(*Config)

	p.config.AllowedPodLabels_ = cfg.ListToMap(p.config.AllowedPodLabels)
	p.config.AllowedNodeLabels_ = cfg.ListToMap(p.config.AllowedNodeLabels)

	p.eventBuf = append(p.eventBuf, '"')
}

func (p *MultilineAction) Stop() {
}

func (p *MultilineAction) Do(event *pipeline.Event) pipeline.ActionResult {
	// todo: do same logic as in join plugin here to send not full logs
	if event.IsTimeoutKind() {
		p.logger.Errorf("can't read next sequential event for k8s pod stream")
		p.resetLogBuf()
		return pipeline.ActionDiscard
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_node").MutateToString(selfNodeName)
	if p.config.OnlyNode {
		return pipeline.ActionPass
	}
	// don't need to unescape/escape log fields cause concatenation of escaped strings is escaped string
	logFragment := event.Root.Dig("log").AsEscapedString()
	if logFragment == "" {
		p.logger.Fatalf("wrong event format, it doesn't contain log field: %s", event.Root.EncodeToString())
		panic("_")
	}

	// docker splits long logs by 16kb chunks, so let's join them
	// look ahead to ensure we won't throw events longer than SplitEventSize
	// lookahead value is much more than 16Kb because json may be escaped
	p.eventSize += event.Size
	predictedLen := p.eventSize + predictionLookahead
	shouldSplit := predictedLen > p.config.SplitEventSize
	logFragmentLen := len(logFragment)
	if logFragment[logFragmentLen-3:logFragmentLen-1] != `\n` && !shouldSplit {
		if len(p.eventBuf) < p.maxEventSize {
			p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1]...)
		} else {
			p.skipNextEvent = true
			p.logger.Errorf("event chunk will be discarded due to max_event_size, source_name=%s", event.SourceName)
		}
		return pipeline.ActionCollapse
	}

	if p.skipNextEvent {
		p.skipNextEvent = false
		p.resetLogBuf()
		return pipeline.ActionDiscard
	}

	ns, pod, container, _, success, podMeta := getMeta(event.SourceName)

	if shouldSplit {
		p.logger.Warnf("too long k8s event found, it'll be split, ns=%s pod=%s container=%s consider increase split_event_size, split_event_size=%d, predicted event size=%d", ns, pod, container, p.config.SplitEventSize, predictedLen)
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
			if len(p.config.AllowedPodLabels_) != 0 {
				_, has := p.config.AllowedPodLabels_[labelName]

				if !has {
					continue
				}
			}

			l := len(event.Buf)
			event.Buf = append(event.Buf, "k8s_pod_label_"...)
			event.Buf = append(event.Buf, labelName...)
			event.Root.AddFieldNoAlloc(event.Root, pipeline.ByteToStringUnsafe(event.Buf[l:])).MutateToString(labelValue)
		}

		for labelName, labelValue := range nodeLabels {
			if len(p.config.AllowedNodeLabels_) != 0 {
				_, has := p.config.AllowedNodeLabels_[labelName]

				if !has {
					continue
				}
			}

			l := len(event.Buf)
			event.Buf = append(event.Buf, "k8s_node_label_"...)
			event.Buf = append(event.Buf, labelName...)
			event.Root.AddFieldNoAlloc(event.Root, pipeline.ByteToStringUnsafe(event.Buf[l:])).MutateToString(labelValue)
		}
	}

	if len(p.eventBuf) > 1 {
		p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1]...)
		p.eventBuf = append(p.eventBuf, '"')

		l := len(event.Buf)
		event.Buf = append(event.Buf, p.eventBuf...)
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToEscapedString(pipeline.ByteToStringUnsafe(event.Buf[l:]))
	}
	p.resetLogBuf()

	return pipeline.ActionPass
}

func (p *MultilineAction) resetLogBuf() {
	p.eventBuf = p.eventBuf[:1]
	p.eventSize = 0
}
