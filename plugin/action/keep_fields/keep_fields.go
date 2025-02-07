package keep_fields

import (
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

	badNodesBuf []*insaneJSON.Node
	fieldPaths  [][]string
	nodePresent []bool
	path        []string
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.StartOld(config, params)
}

func (p *Plugin) StartOld(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	res := p.DoOld(event)
	return res
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

	fields := p.config.Fields
	sort.Slice(fields, func(i, j int) bool {
		return len(fields[i]) < len(fields[j])
	})

	p.fieldPaths = make([][]string, 0, len(fields))

	for i, f1 := range fields {
		if f1 == "" {
			logger.Warn("empty field found")
			continue
		}

		ok := true
		for _, f2 := range fields[:i] {
			if strings.HasPrefix(f1, f2) {
				logger.Warnf("path '%s' included in path '%s'; remove nested path", f1, f2)
				ok = false
				break
			}
		}

		if ok {
			p.fieldPaths = append(p.fieldPaths, cfg.ParseFieldSelector(f1))
		}
	}

	if len(p.fieldPaths) == 0 {
		logger.Warn("no fields will be removed")
	}

	p.nodePresent = make([]bool, len(p.fieldPaths))
	p.path = make([]string, 0, 20)
}

func (p *Plugin) DoNew(event *pipeline.Event) pipeline.ActionResult {
	if len(p.fieldPaths) == 0 || !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for i := range p.nodePresent {
		p.nodePresent[i] = event.Root.Dig(p.fieldPaths[i]...) != nil
	}

	p.badNodesBuf = p.badNodesBuf[:0]
	// пройтись сразу по полям верхнего уровня а не стартовать из корня
	p.collectBadNodes(event.Root.Node)

	for _, node := range p.badNodesBuf {
		node.Suicide()
	}

	return pipeline.ActionPass
}

func (p *Plugin) collectBadNodes(node *insaneJSON.Node) {
	// if node explicitly saved then return
	// if node is not parent of some saved collect it and return
	// check all children

	for i, curPath := range p.fieldPaths {
		if p.nodePresent[i] && equal(p.path, curPath) {
			return
		}
	}

	if !p.isParentOfSaved() {
		p.badNodesBuf = append(p.badNodesBuf, node)
		return
	}

	if !node.IsObject() {
		panic("node is parent of saved so it must be an object")
	}

	for _, child := range node.AsFields() {
		p.path = append(p.path, child.AsString())
		p.collectBadNodes(child)
		p.path = p.path[:len(p.path)-1]
	}
}

/*
Является ли текукщая нода предком одной сохраненных (по конфигу)
*/

// мб префиксное дерево будет быстрее для поиска чтобы не делать лишних equal
func (p *Plugin) isParentOfSaved() bool {
	for i, fieldPath := range p.fieldPaths {
		if !p.nodePresent[i] {
			continue
		}

		if !(len(p.path) < len(fieldPath)) {
			continue
		}

		if equal(p.path, fieldPath[:len(p.path)]) {
			return true
		}
	}

	return false
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, s := range a {
		if s != b[i] {
			return false
		}
	}

	return true
}
