package k8s

import (
	"slices"
	"sync"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/k8s/meta"
	"go.uber.org/zap"
)

type MultilineAction struct {
	config            *Config
	allowedPodLabels  map[string]bool
	allowedNodeLabels map[string]bool

	logger     *zap.SugaredLogger
	controller pipeline.ActionPluginController

	maxEventSize            int
	sourceNameMetaField     string
	cutOffEventByLimit      bool
	cutOffEventByLimitField string

	eventBuf      []byte
	eventSize     int
	skipNextEvent bool
	cutOffEvent   bool
}

const (
	predictionLookahead = 128 * 1024
	newLine             = `\n`
)

func (p *MultilineAction) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger
	p.controller = params.Controller
	p.maxEventSize = params.PipelineSettings.MaxEventSize
	p.sourceNameMetaField = params.PipelineSettings.SourceNameMetaField
	p.cutOffEventByLimit = params.PipelineSettings.CutOffEventByLimit
	p.cutOffEventByLimitField = params.PipelineSettings.CutOffEventByLimitField

	p.config = config.(*Config)

	p.allowedPodLabels = cfg.ListToMap(p.config.AllowedPodLabels)
	p.allowedNodeLabels = cfg.ListToMap(p.config.AllowedNodeLabels)

	p.eventBuf = append(p.eventBuf, '"')
}

func (p *MultilineAction) Stop() {
}

type bytesBuf struct {
	B []byte
}

var escapedStringBufPool = sync.Pool{
	New: func() any {
		return new(bytesBuf)
	},
}

func (p *MultilineAction) Do(event *pipeline.Event) pipeline.ActionResult {
	// todo: do same logic as in join plugin here to send not full logs
	if event.IsTimeoutKind() {
		p.logger.Errorf("can't read next sequential event for k8s pod stream")
		p.resetLogBuf()
		return pipeline.ActionDiscard
	}

	event.Root.AddFieldNoAlloc(event.Root, "k8s_node").MutateToString(meta.SelfNodeName)

	if p.config.OnlyNode {
		return pipeline.ActionPass
	}

	buf := escapedStringBufPool.Get().(*bytesBuf)
	defer escapedStringBufPool.Put(buf)
	buf.B = slices.Grow(buf.B[:0], event.Size)

	// don't need to unescape/escape log fields cause concatenation of escaped strings is escaped string.
	// get escaped string because of CRI format.
	buf.B = event.Root.Dig("log").AppendEscapedString(buf.B)
	logFragment := pipeline.ByteToStringUnsafe(buf.B)
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
	isEnd := logFragment[logFragmentLen-3:logFragmentLen-1] == newLine
	if !isEnd && !shouldSplit {
		sizeAfterAppend := len(p.eventBuf) + len(logFragment)
		// check buffer size before append
		if p.maxEventSize == 0 || sizeAfterAppend < p.maxEventSize {
			p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1]...)
		} else if !p.skipNextEvent {
			if p.controller != nil {
				source := event.SourceName
				if p.sourceNameMetaField != "" {
					// at the moment, all metadata fields have been added to log
					if val := event.Root.Dig(p.sourceNameMetaField).AsString(); val != "" {
						source = val
					}
				}

				p.controller.IncMaxEventSizeExceeded(source)
			}

			// skip event if max_event_size is exceeded
			p.skipNextEvent = true

			if p.cutOffEventByLimit {
				offset := sizeAfterAppend - p.maxEventSize
				p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1-offset]...)
				p.cutOffEvent = true

				p.logger.Errorf("event chunk will be cut off due to max_event_size, source_name=%s", event.SourceName)
			} else {
				p.logger.Errorf("event chunk will be discarded due to max_event_size, source_name=%s", event.SourceName)
			}
		}
		return pipeline.ActionCollapse
	}

	if p.skipNextEvent {
		// wait chunk end
		if !isEnd {
			return pipeline.ActionCollapse
		}
		p.skipNextEvent = false

		if !p.cutOffEvent {
			p.resetLogBuf()
			return pipeline.ActionDiscard
		}
	}

	if shouldSplit {
		p.logger.Warnf("too long k8s event found, it'll be split, source_name=%s consider increase split_event_size, split_event_size=%d, predicted event size=%d", event.SourceName, p.config.SplitEventSize, predictedLen)
	}

	if len(p.eventBuf) > 1 {
		if !p.cutOffEvent {
			p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1]...)
		} else {
			if isEnd {
				p.eventBuf = append(p.eventBuf, newLine...)
			}

			if p.cutOffEventByLimitField != "" {
				event.Root.AddFieldNoAlloc(event.Root, p.cutOffEventByLimitField).MutateToBool(true)
			}
		}
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
	p.cutOffEvent = false
}
