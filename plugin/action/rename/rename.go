package rename

import (
	"gitlab.ozon.ru/sre/file-d/cfg"
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin renames fields of the event. There can be provided unlimited config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false` no renaming will be done in the case of field name collision.

Example:
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      my_object.field.subfield: new_sub_field
    ...

Result event could looks like:
```
{
  "my_object": {
    "field": {
      "new_sub_field":"value"
    }
  },
```
}*/
type Plugin struct {
	paths          [][]string
	names          []string
	preserveFields bool
}

type Config map[string]string

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "rename",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	c := *config.(*Config)
	p.preserveFields = c["override"] == "false"

	c = cfg.UnescapeMap(c)

	for path, name := range c {
		selector := cfg.ParseFieldSelector(path)
		p.paths = append(p.paths, selector)
		p.names = append(p.names, name)
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for index, path := range p.paths {
		if p.preserveFields {
			if event.Root.Dig(p.names[index]) != nil {
				continue
			}
		}

		node := event.Root.Dig(path...)
		if node == nil {
			continue
		}

		node.Suicide()
		event.Root.AddFieldNoAlloc(event.Root, p.names[index]).MutateToNode(node)
	}

	return pipeline.ActionPass
}
