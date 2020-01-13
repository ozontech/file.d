package join

import (
	"regexp"

	"gitlab.ozon.ru/sre/filed/filed"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
)

type Plugin struct {
	controller pipeline.ActionPluginController
	config     *Config

	firstRe *regexp.Regexp
	nextRe  *regexp.Regexp

	isJoining bool
	first     *pipeline.Event
	buff      []byte
}

type Config struct {
	Field string `json:"field"`
	First string `json:"first_re"`
	Next  string `json:"next_re"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
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

	if p.config.Field == "" {
		logger.Fatalf("no field provided for join plugin")
	}

	if p.config.First == "" {
		logger.Fatalf("no %q parameter provided for join plugin", "first_re")
	}
	if p.config.Next == "" {
		logger.Fatalf("no %q parameter provided for join plugin", "next_re")
	}

	r, err := filed.CompileRegex(p.config.First)
	if err != nil {
		logger.Fatalf("can't compile first line regexp: %s", err.Error())
	}
	p.firstRe = r

	r, err = filed.CompileRegex(p.config.Next)
	if err != nil {
		logger.Fatalf("can't compile next line regexp: %s", err.Error())
	}
	p.nextRe = r
}

func (p *Plugin) Stop() {
}

func (p *Plugin) flush() {
	event := p.first
	p.first = nil
	p.isJoining = false

	if event == nil {
		logger.Panicf("first event is nil, why?")
		return
	}

	event.Root.Dig(p.config.Field).MutateToString(string(p.buff))
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

	node := event.Root.Dig(p.config.Field)
	value := node.AsString()

	firstOK := false
	if node.IsString() {
		firstOK = p.firstRe.MatchString(value)
	}

	if firstOK {
		if p.isJoining {
			p.flush()
		}

		p.first = event
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
