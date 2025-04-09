package join_template

import (
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join"
	"github.com/ozontech/file.d/plugin/action/join_template/template"
)

/*{ introduction
Alias to "join" plugin with predefined `start` and `continue` parameters.

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Enable explicit checks without regular expressions (use `fast_check` flag) or
> consider `match_fields` parameter to process only particular events. Check out an example for details.

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

type Plugin struct {
	config *Config

	jp *join.Plugin
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
	Template string `json:"template" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Enable check without regular expressions.
	FastCheck bool `json:"fast_check"` // *
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

	templateNames := strings.Split(strings.TrimSpace(p.config.Template), "|")

	templates := make([]template.Template, 0, len(templateNames))
	for _, name := range templateNames {
		cur, err := template.InitTemplate(name)
		if err != nil {
			logger.Fatalf("failed to init join template \"%s\": %s", templateNames, err)
		}

		templates = append(templates, cur)
	}

	jConfig := &join.Config{
		Field_:       p.config.Field_,
		MaxEventSize: p.config.MaxEventSize,
		FastCheck:    p.config.FastCheck,

		Templates: templates,
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
