package join_template

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join"
)

/*{ introduction
Alias to "join" plugin with predefined `start` and `continue` parameters.

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join_template
      template: go_panic
      field: log
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```
}*/

type joinTemplates map[string]struct {
	startRePat    string
	continueRePat string
}

var templates = joinTemplates{
	"go_panic": {
		startRePat:    "/^(panic:)|(http: panic serving)/",
		continueRePat: "/(^\\s*$)|(goroutine [0-9]+ \\[)|(\\({?[0-9]+x[0-9,a-f]+)|(\\.go:[0-9]+ \\+[0-9]x)|(\\/.*\\.go:[0-9]+)|(\\(...\\))|(main\\.main\\(\\))|(created by .*\\/.*\\.)|(^\\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/",
	},
}

type Plugin struct {
	config *Config

	jp *join.Plugin
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The event field which will be checked for joining with each other.
	Field  cfg.FieldSelector `json:"field" default:"log" required:"true" parse:"selector"` //*
	Field_ []string

	//> @3@4@5@6
	//>
	//> Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.
	MaxEventSize int `json:"max_event_size" default:"0"` //*

	//> @3@4@5@6
	//>
	//> The name of the template. Available templates: `go_panic`.
	Template string `json:"template" required:"true"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "join_template",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	templateName := p.config.Template
	template, ok := templates[templateName]
	if !ok {
		logger.Fatalf("join template \"%s\" not found", templateName)
	}

	startRe, err := cfg.CompileRegex(template.startRePat)
	if err != nil {
		logger.Fatalf("failed to compile regex for template \"%s\": %s", templateName, err.Error())
	}
	continueRe, err := cfg.CompileRegex(template.continueRePat)
	if err != nil {
		logger.Fatalf("failed to compile regex for template \"%s\": %s", templateName, err.Error())
	}

	jConfig := &join.Config{
		Field_:       p.config.Field_,
		MaxEventSize: p.config.MaxEventSize,
		Start_:       startRe,
		Continue_:    continueRe,
	}
	p.jp = &join.Plugin{}
	p.jp.Start(jConfig, params)
}

func (p *Plugin) Stop() {
	p.jp.Stop()
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	return p.jp.Do(event)
}
