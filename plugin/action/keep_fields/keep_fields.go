package keep_fields

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

/*{ introduction
It keeps the list of the event fields and removes others.
}*/

type Plugin struct {
	config *Config

	fieldPaths [][]string
	nodesBuf   []keepNode

	fieldsBuf []string
}

type keepNode struct {
	path []string
	node *insaneJSON.Node
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

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	for _, f := range p.config.Fields {
		p.fieldPaths = append(p.fieldPaths, cfg.ParseFieldSelector(f))
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if false {
		p.doOld(event)
	} else {
		p.doNew(event, 2) // pass 1 or 2
	}

	return pipeline.ActionPass
}

func (p *Plugin) doNew(event *pipeline.Event, alg int) {
	if !event.Root.IsObject() {
		return
	}

	p.nodesBuf = p.nodesBuf[:0]

	// save all fields
	for _, path := range p.fieldPaths {
		node := event.Root.Dig(path...)
		if node != nil {
			p.nodesBuf = append(p.nodesBuf, keepNode{
				path: path,
				node: node,
			})
		}
	}

	switch alg {
	// #1 - MutateToObject() resets all Node.nodes,
	// only backlinks in children to parent
	case 1:
		event.Root.MutateToObject()
		for _, n := range p.nodesBuf {
			pipeline.CreateNestedField(event.Root, n.path).MutateToNode(n.node)
		}
	// #2 - recreate root
	case 2:
		tmp := insaneJSON.Spawn()
		for _, n := range p.nodesBuf {
			pipeline.CreateNestedField(tmp, n.path).MutateToNode(n.node)
		}
		insaneJSON.Release(event.Root)
		event.Root = tmp
	}
}

func (p *Plugin) doOld(event *pipeline.Event) {
	p.fieldsBuf = p.fieldsBuf[:0]

	if !event.Root.IsObject() {
		return
	}

	insaneJSON.Spawn()
	for _, node := range event.Root.AsFields() {
		eventField := node.AsString()
		isInList := false
		for _, pluginField := range p.config.Fields {
			if pluginField == eventField {
				isInList = true
				break
			}
		}
		if !isInList {
			p.fieldsBuf = append(p.fieldsBuf, eventField)
		}
	}

	for _, field := range p.fieldsBuf {
		event.Root.Dig(field).Suicide()
	}
}
