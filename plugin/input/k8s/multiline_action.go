package k8s

import (
	"bytes"
	"unicode/utf8"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

type MultilineAction struct {
	config            *Config
	allowedPodLabels  map[string]bool
	allowedNodeLabels map[string]bool

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

	p.allowedPodLabels = cfg.ListToMap(p.config.AllowedPodLabels)
	p.allowedNodeLabels = cfg.ListToMap(p.config.AllowedNodeLabels)
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
	logFragment := event.Root.Dig("log").AsString()
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
	isEnd := logFragment[logFragmentLen-1] == '\n'
	if !isEnd && !shouldSplit {
		sizeAfterAppend := len(p.eventBuf) + logFragmentLen
		// check buffer size before append
		if p.maxEventSize == 0 || sizeAfterAppend < p.maxEventSize {
			p.eventBuf = append(p.eventBuf, logFragment...)
		} else if !p.skipNextEvent {
			// skip event if max_event_size is exceeded
			p.skipNextEvent = true
			ns, pod, _, _, _ := getMeta(event.SourceName)
			p.logger.Errorf("event chunk will be discarded due to max_event_size, source_name=%s, namespace=%s, pod=%s", event.SourceName, ns, pod)
		}
		return pipeline.ActionCollapse
	}

	if p.skipNextEvent {
		// wait chunk end
		if !isEnd {
			return pipeline.ActionCollapse
		}
		p.skipNextEvent = false
		p.resetLogBuf()
		return pipeline.ActionDiscard
	}

	ns, pod, container, success, podMeta := getMeta(event.SourceName)

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
			if len(p.allowedPodLabels) != 0 {
				_, has := p.allowedPodLabels[labelName]

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
			if len(p.allowedNodeLabels) != 0 {
				_, has := p.allowedNodeLabels[labelName]

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

	if len(p.eventBuf) > 0 {
		p.eventBuf = append(p.eventBuf, logFragment...)
		event.Root.AddFieldNoAlloc(event.Root, "log").MutateToEscapedString(
			pipeline.ByteToStringUnsafe(escapeBytes(p.eventBuf)))
	}
	p.resetLogBuf()

	return pipeline.ActionPass
}

func (p *MultilineAction) resetLogBuf() {
	p.eventBuf = p.eventBuf[:0]
	p.eventSize = 0
}

const hex = "0123456789abcdef"

// escapeBytes is based on escapeString from insaneJSON package.
func escapeBytes(bs []byte) []byte {
	out := make([]byte, 0, len(bs))

	if !shouldEscape(bs) {
		out = append(out, '"')
		out = append(out, bs...)
		out = append(out, '"')
		return out
	}

	out = append(out, '"')
	start := 0
	for i := 0; i < len(bs); {
		if b := bs[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			if start < i {
				out = append(out, bs[start:i]...)
			}
			switch b {
			case '\\', '"':
				out = append(out, '\\', b)
			case '\n':
				out = append(out, "\\n"...)
			case '\r':
				out = append(out, "\\r"...)
			case '\t':
				out = append(out, "\\t"...)
			default:
				out = append(out, "\\u00"...)
				out = append(out, hex[b>>4], hex[b&0xf])
			}
			i++
			start = i
			continue
		}

		c, size := utf8.DecodeRune(bs[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				out = append(out, bs[start:i]...)
			}
			out = append(out, "\\ufffd"...)
			i += size
			start = i
			continue
		}

		if c == '\u2028' || c == '\u2029' {
			if start < i {
				out = append(out, bs[start:i]...)
			}
			out = append(out, "\\u202"...)
			out = append(out, hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(bs) {
		out = append(out, bs[start:]...)
	}
	out = append(out, '"')

	return out
}

func shouldEscape(b []byte) bool {
	if bytes.IndexByte(b, '"') >= 0 || bytes.IndexByte(b, '\\') >= 0 {
		return true
	}

	l := len(b)
	for i := 0; i < l; i++ {
		if b[i] < 0x20 {
			return true
		}
	}

	return false
}
