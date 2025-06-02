package join_template

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join"
	"github.com/ozontech/file.d/plugin/action/join_template/template"
	"go.uber.org/zap"
)

/*{ introduction
Alias to `join` plugin with predefined fast (regexes not used) `start` and `continue` checks.
Use `do_if` or `match_fields` to prevent extra checks and reduce CPU usage.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
      - type: join_template
        template: go_panic
        field: log
        do_if:
          field: stream
          op: equal
          values:
            - stderr # apply only for events which was written to stderr to save CPU time
    ...
```
}*/

type Plugin struct {
	config *Config

	jp *join.Plugin

	templates      []template.Template
	curTemplateIdx int
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which will be checked for joining with each other.
	Field  cfg.FieldSelector `json:"field" default:"log" required:"true" parse:"selector"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.
	MaxEventSize int `json:"max_event_size" default:"0"` // *

	// > @3@4@5@6
	// >
	// > The name of the template. Available templates: `go_panic`, `cs_exception`, `go_data_race`.
	// > Deprecated; use `templates` instead.
	Template string `json:"template"` // *

	// > @3@4@5@6
	// >
	// > Names of templates. Available templates: `go_panic`, `cs_exception`, `go_data_race`.
	Templates []string `json:"templates"` // *
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
	logger := params.Logger.Desugar()
	p.config = config.(*Config)

	oneTemplate := p.config.Template != ""
	manyTemplates := len(p.config.Templates) > 0

	var templates []template.Template

	if oneTemplate && manyTemplates {
		logger.Warn("field 'template' is deprecated and suppressed by field 'templates'")
	}

	switch {
	case manyTemplates:
		for _, cur := range p.config.Templates {
			result, err := template.InitTemplate(cur)
			if err != nil {
				logger.Fatal("failed to init join template", zap.String("name", cur), zap.Error(err))
			}
			templates = append(templates, result)
		}
	case oneTemplate:
		result, err := template.InitTemplate(p.config.Template)
		if err != nil {
			logger.Fatal("failed to init join template", zap.String("name", p.config.Template), zap.Error(err))
		}
		templates = append(templates, result)
	default:
		logger.Fatal("either field 'template' or field 'templates' must be non empty")
	}

	p.templates = templates
	p.curTemplateIdx = -1

	jConfig := &join.Config{
		Field_:       p.config.Field_,
		MaxEventSize: p.config.MaxEventSize,

		FirstCheck: p.firstCheck,
		NextCheck:  p.nextCheck,
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

func (p *Plugin) firstCheck(value string) bool {
	for i, cur := range p.templates {
		if cur.StartCheck(value) {
			p.curTemplateIdx = i
			return true
		}
	}

	return false
}

func (p *Plugin) nextCheck(value string) bool {
	curTemplate := p.templates[p.curTemplateIdx]
	result := curTemplate.ContinueCheck(value)

	if curTemplate.Negate {
		result = !result
	}

	return result
}
