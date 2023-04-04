package rename

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It renames the fields of the event. You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false`, the field won't be renamed in the case of field name collision.
Sequence of rename operations isn't guaranteed. Use different actions for prioritization.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      my_object.field.subfield: new_sub_field
    ...
```

The resulting event could look like:
```yaml
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

type Config map[string]any

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
	sharedConfig := *config.(*Config)
	localConfig := make(map[string]any, len(sharedConfig)) // clone shared config to be able to modify it
	for k, v := range sharedConfig {
		localConfig[k] = v
	}

	p.preserveFields = localConfig["override"] == nil || !localConfig["override"].(bool)

	delete(localConfig, "override")
	m := cfg.UnescapeMap(localConfig)

	for path, name := range m {
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
