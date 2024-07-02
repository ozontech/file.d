package remove_fields

import (
	"slices"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It removes the list of the event fields and keeps others.
}*/

type Plugin struct {
	config     *Config
	fieldPaths [][]string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of the fields to remove. Nested fields supported.
	Fields []string `json:"fields"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "remove_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the remove fields plugin")
	}

	fieldPaths := make([][]string, 0, len(p.config.Fields))
	for i, field := range p.config.Fields {
		if field == "" {
			logger.Fatalf("empty field; pos = %d", i)
		}

		fieldPath := cfg.ParseFieldSelector(field)
		if len(fieldPath) == 0 {
			logger.Fatalf("empty field selector parsed; field pos = %d", i)
		}

		fieldPaths = append(fieldPaths, fieldPath)
	}

	p.fieldPaths = make([][]string, 0, len(fieldPaths))

	for i, p1 := range fieldPaths {
		ok := true
		for j, p2 := range fieldPaths {
			if i == j {
				continue
			}

			if slices.Equal(p1, p2) {
				logger.Warnf(
					"duplicate path '%s' found; remove extra occurrence",
					strings.Join(p1, "."),
				)
				continue
			}

			if includes(p2, p1) {
				logger.Warnf(
					"'%s' path includes '%s' path; remove nested path",
					strings.Join(p2, "."),
					strings.Join(p1, "."),
				)
				ok = false
			}
		}

		if ok {
			p.fieldPaths = append(p.fieldPaths, p1)
		}
	}
}

func includes(a, b []string) bool {
	if !(len(a) <= len(b)) {
		return false
	}

	result := true
	for i := 0; i < len(a); i++ {
		result = result && a[i] == b[i]
	}

	return result
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for _, fieldPath := range p.fieldPaths {
		event.Root.Dig(fieldPath...).Suicide()
	}

	return pipeline.ActionPass
}
