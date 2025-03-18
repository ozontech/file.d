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

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if len(p.parsedFieldsRoot.children) == 0 || !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	fpNode := p.parsedFieldsRoot
	eventNode := event.Root.Node
	depth := 0
	// check root nodes first
	for _, node := range eventNode.AsFields() {
		eventField := node.AsString()
		if childNode, ok := fpNode.children[eventField]; ok {
			// no child nodes in input path, found target node
			if len(childNode.children) == 0 {
				continue
			}
			// check nested fields, if exists, keep node
			if exists := p.traverseFieldsTree(childNode, eventNode.Dig(eventField), depth+1); exists {
				continue
			}
		}
		// nodes to remove
		p.fieldsDepthSlice[depth] = append(p.fieldsDepthSlice[depth], eventField)
	}

	for _, field := range p.fieldsDepthSlice[depth] {
		event.Root.Dig(field).Suicide()
	}

	// clean fields depth slice for the next iteration
	for i := range p.fieldsDepthSlice {
		p.fieldsDepthSlice[i] = p.fieldsDepthSlice[i][:0]
	}

	return pipeline.ActionPass
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

type fieldPathNode struct {
	name     string
	children map[string]*fieldPathNode
}

func newFieldPathNode(name string) *fieldPathNode {
	return &fieldPathNode{
		name:     name,
		children: make(map[string]*fieldPathNode),
	}
}

func (p *Plugin) StartTraverseTree() {
	p.parsedFieldsRoot = newFieldPathNode("") // root node

	fieldMaxDepth := 0
	for _, fieldPath := range p.fieldPaths {
		fieldMaxDepth = max(fieldMaxDepth, len(fieldPath))

		curNode := p.parsedFieldsRoot
		for _, field := range fieldPath {
			nextNode, ok := curNode.children[field]
			if !ok {
				nextNode = newFieldPathNode(field)
				curNode.children[field] = nextNode
			}
			curNode = nextNode
		}
	}

	p.fieldsDepthSlice = make([][]string, fieldMaxDepth)
	for i := 0; i < fieldMaxDepth; i++ {
		p.fieldsDepthSlice[i] = make([]string, 0, 100)
	}
}

func (p *Plugin) traverseFieldsTree(fpNode *fieldPathNode, eventNode *insaneJSON.Node, depth int) bool {
	// no child nodes in input path, found target node
	if len(fpNode.children) == 0 {
		return true
	}
	// cannot go further, nested target field does not exist
	if !eventNode.IsObject() {
		return false
	}
	shouldPreserveNode := false
	for _, node := range eventNode.AsFields() {
		eventField := node.AsString()
		if childNode, ok := fpNode.children[eventField]; ok {
			if len(childNode.children) == 0 {
				shouldPreserveNode = true
				continue
			}
			if exists := p.traverseFieldsTree(childNode, eventNode.Dig(eventField), depth+1); exists {
				shouldPreserveNode = true
				continue
			}
		}
		p.fieldsDepthSlice[depth] = append(p.fieldsDepthSlice[depth], eventField)
	}
	if shouldPreserveNode {
		// remove all unnecessary fields from current node, if the current node should be preserved
		for _, field := range p.fieldsDepthSlice[depth] {
			eventNode.Dig(field).Suicide()
		}
		p.fieldsDepthSlice[depth] = p.fieldsDepthSlice[depth][:0]
	}
	return shouldPreserveNode
}
