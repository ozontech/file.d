package keep_fields

import (
	"sort"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It keeps the list of the event fields and removes others.
}*/

type Plugin struct {
	config *Config

	fieldPaths [][]string

	parsedFieldsRoot *fieldPathNode
	fieldsDepthSlice [][]string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of the fields to keep.
	Fields []string `json:"fields"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "keep_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the keep fields plugin")
	}

	p.fieldPaths = parseNestedFields(p.config.Fields)

	if len(p.fieldPaths) == 0 {
		logger.Warn("all fields will be removed")
	}

	p.StartTraverseTree()
}

// TODO: replace with cfg.ParseNestedFields
func parseNestedFields(rawPaths []string) [][]string {
	sort.Slice(rawPaths, func(i, j int) bool {
		return len(rawPaths[i]) < len(rawPaths[j])
	})

	result := make([][]string, 0, len(rawPaths))

	for i, f1 := range rawPaths {
		if f1 == "" {
			logger.Warn("empty field found")
			continue
		}

		ok := true
		for _, f2 := range rawPaths[:i] {
			if f1 == f2 {
				logger.Warnf("path '%s' duplicates", f1)
				ok = false
				break
			}

			if strings.HasPrefix(f1, f2+".") {
				logger.Warnf("path '%s' included in path '%s'; remove nested path", f1, f2)
				ok = false
				break
			}
		}

		if ok {
			result = append(result, cfg.ParseFieldSelector(f1))
		}
	}

	return result
}
