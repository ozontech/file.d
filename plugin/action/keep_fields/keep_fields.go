package keep_fields

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It keeps the list of the event fields and removes others.
}*/

type Plugin struct {
	config    *Config
	fieldsBuf []string

	nested bool

	fieldPaths [][]string
	path       []string

	arrayChecker *arrayChecker
	treeChecker  *treeChecker
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

func (p *Plugin) StartOld(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) DoOld(event *pipeline.Event) pipeline.ActionResult {
	p.fieldsBuf = p.fieldsBuf[:0]

	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for _, node := range event.Root.AsFields() {
		eventField := node.AsString()
		if find(p.config.Fields, eventField) == -1 {
			p.fieldsBuf = append(p.fieldsBuf, eventField)
		}
	}

	for _, field := range p.fieldsBuf {
		event.Root.Dig(field).Suicide()
	}

	return pipeline.ActionPass
}

func find(a []string, s string) int {
	for i, elem := range a {
		if elem == s {
			return i
		}
	}

	return -1
}

func (p *Plugin) StartNew(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the keep fields plugin")
	}

	p.fieldPaths = parsePaths(p.config.Fields)

	if len(p.fieldPaths) == 0 {
		logger.Warn("all fields will be removed")
	}

	for _, path := range p.fieldPaths {
		p.nested = p.nested || len(path) >= 2
	}

	p.path = make([]string, 0, 20)

	p.arrayChecker = newArrayChecker(p.fieldPaths)
	p.treeChecker = newPrefixTree(p.fieldPaths)
}

// Parse paths and skip nested path to reduce paths count.
// Func sorts paths by length iterates over shorter paths
// and checks for current path if duplicate or some prefix found.
func parsePaths(rawPaths []string) [][]string {
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

func (p *Plugin) DoNewWithArray(event *pipeline.Event) pipeline.ActionResult {
	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	if !p.nested {
		p.fieldsBuf = p.fieldsBuf[:0]

		for _, node := range event.Root.AsFields() {
			eventField := node.AsString()
			if find(p.config.Fields, eventField) == -1 {
				p.fieldsBuf = append(p.fieldsBuf, eventField)
			}
		}

		for _, field := range p.fieldsBuf {
			event.Root.Dig(field).Suicide()
		}

		return pipeline.ActionPass
	}

	p.arrayChecker.startChecks(event.Root)

	for _, child := range event.Root.AsFields() {
		eventField := child.AsString()
		p.path = append(p.path, eventField)
		p.eraseBadNodesArrChecker(event.Root.Node.Dig(eventField))
		p.path = p.path[:len(p.path)-1]
	}

	p.arrayChecker.finishChecks()

	return pipeline.ActionPass
}

func (p *Plugin) eraseBadNodesArrChecker(node *insaneJSON.Node) {
	status := p.arrayChecker.check(p.path)
	switch status {
	case saved:
		return
	case parentOfSaved:
		if !node.IsObject() {
			panic("node is parent of saved so it must be an object")
		}

		for _, child := range node.AsFields() {
			p.path = append(p.path, child.AsString())
			p.eraseBadNodesArrChecker(child.AsFieldValue())
			p.path = p.path[:len(p.path)-1]
		}
	case unsaved:
		node.Suicide()
	default:
		panic(fmt.Sprintf("unknown node status: %d", status))
	}
}

func (p *Plugin) eraseBadNodesTreeChecker(node *insaneJSON.Node) {
	status := p.treeChecker.check(p.path)
	switch status {
	case saved:
		return
	case parentOfSaved:
		if !node.IsObject() {
			panic("node is parent of saved so it must be an object")
		}

		for _, child := range node.AsFields() {
			p.path = append(p.path, child.AsString())
			p.eraseBadNodesTreeChecker(child.AsFieldValue())
			p.path = p.path[:len(p.path)-1]
		}
	case unsaved:
		node.Suicide()
	default:
		panic(fmt.Sprintf("unknown node status: %d", status))
	}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.StartNew(config, params)
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	res := p.DoNewWithArray(event)
	return res
}

func (p *Plugin) DoNewWithTree(event *pipeline.Event) pipeline.ActionResult {
	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	if !p.nested {
		p.fieldsBuf = p.fieldsBuf[:0]

		for _, node := range event.Root.AsFields() {
			eventField := node.AsString()
			if find(p.config.Fields, eventField) == -1 {
				p.fieldsBuf = append(p.fieldsBuf, eventField)
			}
		}

		for _, field := range p.fieldsBuf {
			event.Root.Dig(field).Suicide()
		}

		return pipeline.ActionPass
	}

	p.treeChecker.startChecking(event.Root)

	for _, child := range event.Root.AsFields() {
		eventField := child.AsString()
		p.path = append(p.path, eventField)
		p.eraseBadNodesTreeChecker(event.Root.Node.Dig(eventField))
		p.path = p.path[:len(p.path)-1]
	}

	p.treeChecker.finishChecking()

	return pipeline.ActionPass
}
