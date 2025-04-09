package join

import (
	"regexp"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join_template/template"
	"go.uber.org/zap"
)

/*{ introduction
It makes one big event from the sequence of the events.
It is useful for assembling back together "exceptions" or "panics" if they were written line by line.
Also known as "multiline".

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join
      field: log
      start: '/^(panic:)|(http: panic serving)/'
      continue: '/(^\s*$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```
}*/

/*{ understanding
**No joining:**
```
event 1
event 2 – matches start regexp
event 3
event 4 – matches continue regexp
event 5
```

**Events `event 2` and `event 3` will be joined:**
```
event 1
event 2 – matches start regexp
event 3 – matches continue regexp
event 4
```

**Events from `event 2` to `event N` will be joined:**
```
event 1
event 2 matches start regexp
event 3 matches continue regexp
event ... matches continue regexp
event N matches continue regexp
event N+1
```
}*/

type Plugin struct {
	controller pipeline.ActionPluginController
	config     *Config

	isJoining    bool
	initial      *pipeline.Event
	buff         []byte
	maxEventSize int
	negate       bool

	templateID int

	logger *zap.SugaredLogger
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which will be checked for joining with each other.
	Field  cfg.FieldSelector `json:"field" required:"true" parse:"selector"` // *
	Field_ []string

	// Special flag for join_template plugin;
	// it allows to check strings without regexp
	FastCheck bool

	// > @3@4@5@6
	// >
	// > A regexp which will start the join sequence.
	Start  cfg.Regexp `json:"start" required:"true" parse:"regexp"` // *
	Start_ *regexp.Regexp

	// > @3@4@5@6
	// >
	// > A regexp which will continue the join sequence.
	Continue  cfg.Regexp `json:"continue" required:"true" parse:"regexp"` // *
	Continue_ *regexp.Regexp

	// > @3@4@5@6
	// >
	// > Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.
	MaxEventSize int `json:"max_event_size" default:"0"` // *

	// > @3@4@5@6
	// >
	// > Negate match logic for Continue (lets you implement negative lookahead while joining lines)
	Negate bool `json:"negate" default:"false"` // *

	Templates []template.Template
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "join",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.controller = params.Controller
	p.config = config.(*Config)
	p.isJoining = false
	p.buff = make([]byte, 0, params.PipelineSettings.AvgEventSize)
	p.maxEventSize = p.config.MaxEventSize
	p.negate = p.config.Negate
	p.logger = params.Logger
}

func (p *Plugin) Stop() {
}

func (p *Plugin) flush() {
	event := p.initial
	p.initial = nil
	p.isJoining = false
	p.templateID = -1

	if event == nil {
		p.logger.Panicf("first event is nil, why?")
		return
	}

	event.Root.Dig(p.config.Field_...).MutateToString(string(p.buff))
	p.controller.Propagate(event)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		if !p.isJoining {
			p.logger.Panicf("timeout without joining, why?")
		}
		p.flush()
		return pipeline.ActionDiscard
	}

	node := event.Root.Dig(p.config.Field_...)
	if node == nil {
		if p.isJoining {
			p.flush()
		}
		return pipeline.ActionPass
	}

	value := node.AsString()

	firstOK := false
	templateID := -1
	if node.IsString() {
		if len(p.config.Templates) == 0 {
			firstOK = p.config.Start_.MatchString(value)
		} else {
			templateID = p.getStartingTemplateID(value)
			firstOK = templateID != -1
		}
	}

	if firstOK {
		if p.isJoining {
			p.flush()
		}

		p.initial = event
		p.isJoining = true
		p.buff = append(p.buff[:0], value...)
		p.templateID = templateID
		return pipeline.ActionHold
	}

	if p.isJoining {
		if p.isNextOK(value) {
			if p.maxEventSize == 0 || len(p.buff) < p.maxEventSize {
				p.buff = append(p.buff, value...)
			}
			return pipeline.ActionCollapse
		}
	}

	if p.isJoining {
		p.flush()
	}
	return pipeline.ActionPass
}

func (p *Plugin) isNextOK(value string) bool {
	result := false

	if len(p.config.Templates) == 0 {
		result = p.config.Continue_.MatchString(value)

		if p.negate {
			result = !result
		}
	} else {
		curTemplate := p.getCurrentTemplate()
		if p.config.FastCheck {
			result = curTemplate.ContinueCheck(value)
		} else {
			result = curTemplate.ContinueRe.MatchString(value)
		}

		if curTemplate.Negate {
			result = !result
		}
	}

	return result
}

func (p *Plugin) getStartingTemplateID(value string) int {
	for i, cur := range p.config.Templates {
		res := false
		if p.config.FastCheck {
			res = cur.StartCheck(value)
		} else {
			res = cur.StartRe.MatchString(value)
		}

		if res {
			return i
		}
	}

	return -1
}

func (p *Plugin) getCurrentTemplate() template.Template {
	return p.config.Templates[p.templateID]
}
