# Discard action plugin
Plugin simply drops event. Used in a combination with `match_fields` parameters to filter out events.

*Example discarding informational and debug logs:*
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: discard
      match_fields:
        level: /info|debug/
    ...
```

} */
type Plugin struct {
}

type Config struct {
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "discard",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
}

func (p *Plugin) Stop() {

}

func (p *Plugin) Do(_ *pipeline.Event) pipeline.ActionResult {
	return pipeline.ActionDiscard
}


> No config params

##
 *Generated using **insane-doc***