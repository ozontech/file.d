package join

import (
	"regexp"

	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin also known as "multiline" makes one big event from event sequence.
Useful for assembling back together "exceptions" or "panics" if they was written line by line.

> ⚠ Parsing all event flow could be very CPU intensive because plugin uses regular expressions.
> Consider `match_fields` parameter to apply it only for particular events. Check out example for details.

### Understanding start/continue regexps

No joining:
```
event 1
event 2 – matches start regexp
event 3
event 4 – matches continue regexp
event 5
```

Events `event 2` and `event 3` will be joined:
```
event 1
event 2 – matches start regexp
event 3 – matches continue regexp
event 4
```

Events from `event 2` to `event N` will be joined:
```
event 1
event 2 matches start regexp
event 3 matches continue regexp
event ... matches continue regexp
event N matches continue regexp
event N+1
```
<br/>

Example of joining Golang panics:
```
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

type Plugin struct {
	controller pipeline.ActionPluginController
	config     *Config

	isJoining bool
	initial   *pipeline.Event
	buff      []byte
}

//! config /json:\"([a-z_]+)\"/ #2 /default:\"([^"]+)\"/ /(required):\"true\"/  /options:\"([^"]+)\"/
//^ _ _ code /`default=%s`/ code /`options=%s`/
type Config struct {
	//> @3 @4 @5 @6
	//>
	//> Field of event which will be analyzed for joining with each other.
	Field  cfg.FieldSelector `json:"field" required:"true" parse:"selector"` //*
	Field_ []string

	//> @3 @4 @5 @6
	//>
	//> Regexp which will start join sequence.
	Start  cfg.Regexp `json:"start" required:"true" parse:"regexp"` //*
	Start_ regexp.Regexp

	//> @3 @4 @5 @6
	//>
	//> Regexp which will continue join sequence.
	Continue  cfg.Regexp `json:"continue" required:"true" parse:"regexp"` //*
	Continue_ regexp.Regexp
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
	p.buff = make([]byte, 0, params.PipelineSettings.AvgLogSize)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) flush() {
	event := p.initial
	p.initial = nil
	p.isJoining = false

	if event == nil {
		logger.Panicf("first event is nil, why?")
		return
	}

	event.Root.Dig(p.config.Field_...).MutateToString(string(p.buff))
	p.controller.Propagate(event)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		if !p.isJoining {
			logger.Panicf("timeout without joining, why?")
		}
		p.flush()
		return pipeline.ActionDiscard
	}

	node := event.Root.Dig(p.config.Field_...)
	value := node.AsString()

	firstOK := false
	if node.IsString() {
		firstOK = p.firstRe.MatchString(value)
	}

	if firstOK {
		if p.isJoining {
			p.flush()
		}

		p.initial = event
		p.isJoining = true
		p.buff = append(p.buff[:0], value...)
		return pipeline.ActionHold
	}

	if p.isJoining {
		nextOK := p.nextRe.MatchString(value)
		if nextOK {
			p.buff = append(p.buff, value...)
			return pipeline.ActionCollapse
		}
	}

	if p.isJoining {
		p.flush()
	}
	return pipeline.ActionPass
}
