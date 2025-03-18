package keep_fields

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
)

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
	// parsedFields := make([][]string, len(p.config.Fields))
	for _, f := range p.config.Fields {
		parsedF := cfg.ParseFieldSelector(f)
		// parsedFields = append(parsedFields, parsedF)
		if len(parsedF) > fieldMaxDepth {
			fieldMaxDepth = len(parsedF)
		}
		curNode := p.parsedFieldsRoot
		for _, field := range parsedF {
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

func (p *Plugin) DoNewWithTraverseTree(event *pipeline.Event) pipeline.ActionResult {
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
