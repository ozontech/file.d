package file_multiline

import (
	"slices"
	"sync"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It joins split stdout/container log chunks into a single event.

Docker/CRI splits long logs by 16kb chunks. Use this action with [file input plugin](/plugin/input/file/README.md)
when reading container logs from files.

> ⚠ Place it **before** `json_decode`, `decode`, and other actions that expect a complete field value. For Docker JSON logs use `field: log`.

**Example:**
```yaml
pipelines:
  example_pipeline:
    settings:
      decoder: json
    input:
      type: file
      offsets_file: /data/offsets.yaml
      watching_dir: /var/log/containers/
    actions:
    - type: file_multiline
      field: log
      split_event_size: 1000000
    - type: json_decode
      field: log
```

### How it works
1. Reads the configured field (`log` by default) from each sequential event in the same file stream.
2. If the value does **not** end with a real newline character `\n`, the chunk is buffered and the plugin waits for the next event. Further actions are not run yet.
3. When the next chunk arrives, it is appended to the buffer and the end is checked again.
4. When the value ends with `\n`, all buffered chunks are merged into one field and a single event is passed downstream.
5. If the joined event exceeds `split_event_size`, it may be split forcibly even without `\n` at the end.
6. If no continuation arrives within the pipeline `event_timeout` (default `30s`), the buffer is reset and the partial event is discarded.
}*/

type Plugin struct {
	config *Config
	field  []string

	logger                  *zap.SugaredLogger
	controller              pipeline.ActionPluginController
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

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which will be joined.
	Field  cfg.FieldSelector `json:"field" default:"log" parse:"selector"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Docker splits long logs by 16kb chunks. The plugin joins them back, but if an event is longer than this value in bytes, it will be split after all.
	// > > Due to the optimization process it's not a strict rule. Events may be split even if they won't exceed the limit.
	SplitEventSize int `json:"split_event_size" default:"1000000"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "file_multiline",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger
	p.controller = params.Controller
	p.maxEventSize = params.PipelineSettings.MaxEventSize
	p.sourceNameMetaField = params.PipelineSettings.SourceNameMetaField
	p.cutOffEventByLimit = params.PipelineSettings.CutOffEventByLimit
	p.cutOffEventByLimitField = params.PipelineSettings.CutOffEventByLimitField

	p.config = config.(*Config)
	p.field = p.config.Field_
	if len(p.field) == 0 {
		p.field = cfg.ParseFieldSelector("log")
	}

	p.eventBuf = append(p.eventBuf, '"')
}

func (p *Plugin) Stop() {
}

type bytesBuf struct {
	B []byte
}

var escapedStringBufPool = sync.Pool{
	New: func() any {
		return new(bytesBuf)
	},
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		p.logger.Errorf("can't read next sequential event for file stream, source=%s", event.SourceName)
		p.resetLogBuf()
		return pipeline.ActionDiscard
	}

	logNode := event.Root.Dig(p.field...)
	if logNode == nil {
		if len(p.eventBuf) > 1 {
			p.logger.Warnf("field %v is missing while joining split log, source=%s", p.field, event.SourceName)
			p.resetLogBuf()
		}
		return pipeline.ActionPass
	}

	buf := escapedStringBufPool.Get().(*bytesBuf)
	defer escapedStringBufPool.Put(buf)
	buf.B = slices.Grow(buf.B[:0], event.Size)

	buf.B = logNode.AppendEscapedString(buf.B)
	logFragment := pipeline.ByteToStringUnsafe(buf.B)
	if logFragment == "" {
		return pipeline.ActionPass
	}

	logFragmentLen := len(logFragment)
	if logFragmentLen < 3 {
		return pipeline.ActionPass
	}

	logValue := logNode.AsString()

	p.eventSize += event.Size
	predictedLen := p.eventSize + predictionLookahead
	shouldSplit := predictedLen > p.config.SplitEventSize
	// check the real trailing newline of the log line, not escaped JSON suffix:
	// chunk may end with literal "\r\n" text and falsely match the old escaped check.
	isEnd := len(logValue) > 0 && logValue[len(logValue)-1] == '\n'
	if !isEnd && !shouldSplit {
		sizeAfterAppend := len(p.eventBuf) + len(logFragment)
		if p.maxEventSize == 0 || sizeAfterAppend < p.maxEventSize {
			p.eventBuf = append(p.eventBuf, logFragment[1:logFragmentLen-1]...)
		} else if !p.skipNextEvent {
			source := event.SourceName
			if p.sourceNameMetaField != "" {
				if val := event.Root.Dig(p.sourceNameMetaField).AsString(); val != "" {
					source = val
				}
			}
			if p.controller != nil {
				p.controller.IncMaxEventSizeExceeded(source)
			}

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
		p.logger.Warnf("too long event found, it'll be split, source=%s consider increase split_event_size, split_event_size=%d, predicted event size=%d", event.SourceName, p.config.SplitEventSize, predictedLen)
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
		pipeline.CreateNestedField(event.Root, p.field).MutateToEscapedString(pipeline.ByteToStringUnsafe(event.Buf[l:]))
	}
	p.resetLogBuf()

	return pipeline.ActionPass
}

func (p *Plugin) resetLogBuf() {
	p.eventBuf = p.eventBuf[:1]
	p.eventSize = 0
	p.cutOffEvent = false
}
