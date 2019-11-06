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
	firstRe    *regexp.Regexp
	nextRe     *regexp.Regexp
	isNext     bool

	firstEvent *pipeline.Event
	buff       []byte
}

type Config struct {
	Field string `json:"field"`
	First string `json:"first_re"`
	Next  string `json:"next_re"`
}

func init() {
	filed.DefaultPluginRegistry.RegisterAction(&pipeline.PluginInfo{
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
	p.isNext = false
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
	if p.firstEvent == nil {
		logger.Panicf("first event is nil, why?")
	}

	p.firstEvent.Root.Dig(p.config.Field).MutateToString(string(p.buff))
	p.controller.Propagate(p.firstEvent)
	p.firstEvent = nil
	p.isNext = false
}

func (p *Plugin) handleFirstEvent(event *pipeline.Event, value string) {
	if p.isNext {
		p.flush()
	}

	p.firstEvent = event
	p.isNext = true
	p.buff = append(p.buff[:0], value...)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		p.flush()
		return pipeline.ActionDiscard
	}

	value := event.Root.Dig(p.config.Field)

	if !value.IsString() {
		if p.isNext {
			p.flush()
		}
		return pipeline.ActionPass
	}

	valStr := value.AsString()
	isFirst := p.firstRe.MatchString(valStr)
	if isFirst {
		p.handleFirstEvent(event, valStr)
		return pipeline.ActionHold
	}

	if p.isNext {
		isNext := p.nextRe.MatchString(valStr)
		if isNext {
			p.buff = append(p.buff, valStr...)
			return pipeline.ActionCollapse
		}

		p.flush()
	}

	return pipeline.ActionPass
}
