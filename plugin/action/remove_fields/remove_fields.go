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
	for _, field := range p.config.Fields {
		fieldPath := cfg.ParseFieldSelector(field)

		// Setting empty path leads to digging immortal root node
		if len(fieldPath) == 0 {
			logger.Fatalf("can't remove entire object; use discard plugin instead")
		}

		fieldPaths = append(fieldPaths, fieldPath)
	}

	ok := make([]bool, len(fieldPaths))
	for i := range ok {
		ok[i] = true
	}

	for i := range fieldPaths {
		for j := range fieldPaths {
			if i == j {
				continue
			}

			if slices.Equal(fieldPaths[i], fieldPaths[j]) {
				logger.Warnf(
					"duplicate path '%s' found; remove extra occurrence",
					strings.Join(fieldPaths[i], "."),
				)
				continue
			}

			if includes(fieldPaths[i], fieldPaths[j]) {
				logger.Warnf(
					"'%s' path includes '%s' path; remove nested path",
					strings.Join(fieldPaths[i], "."),
					strings.Join(fieldPaths[j], "."),
				)
				ok[j] = false
			}
		}
	}

	p.fieldPaths = make([][]string, 0, len(fieldPaths))
	for i, fieldPath := range fieldPaths {
		if ok[i] {
			p.fieldPaths = append(p.fieldPaths, fieldPath)
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
