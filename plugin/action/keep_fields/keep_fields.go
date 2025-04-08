package keep_fields

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It keeps the list of the event fields and removes others.
Nested fields supported: list subfield names separated with dot.
Example:
```
fields: ["a.b.f1", "c"]
# event before processing
{
    "a":{
        "b":{
            "f1":1,
            "f2":2
        }
    },
    "c":0,
    "d":0
}

# event after processing
{
    "a":{
        "b":{
            "f1":1
        }
    },
    "c":0
}

```

NOTE: if `fields` param contains nested fields they will be removed.
For example `fields: ["a.b", "a"]` gives the same result as `fields: ["a"]`.
See `cfg.ParseNestedFields`.
}*/

type Plugin struct {
	config *Config
	logger *zap.Logger

	fieldPaths [][]string

	parsedFieldsRoot fieldPathNode
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

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger.Desugar()

	p.config = config.(*Config)
	if p.config == nil {
		p.logger.Panic("config is nil for the keep fields plugin")
	}

	var err error
	p.fieldPaths, err = cfg.ParseNestedFields(p.config.Fields)
	if err != nil {
		p.logger.Fatal("can't parse nested fields", zap.Error(err))
	}

	p.parsedFieldsRoot = newFieldPathNode() // root node

	fieldMaxDepth := 0
	for _, fieldPath := range p.fieldPaths {
		fieldMaxDepth = max(fieldMaxDepth, len(fieldPath))

		curNode := p.parsedFieldsRoot
		for _, field := range fieldPath {
			nextNode, ok := curNode.children[field]
			if !ok {
				nextNode = newFieldPathNode()
				curNode.children[field] = nextNode
			}

			curNode = nextNode
		}
	}

	// buffer to store fields to delete
	p.fieldsDepthSlice = make([][]string, fieldMaxDepth)
	for i := 0; i < fieldMaxDepth; i++ {
		p.fieldsDepthSlice[i] = make([]string, 0, 100)
	}
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	p.traverseFieldsTree(p.parsedFieldsRoot, event.Root.Node, 0)

	return pipeline.ActionPass
}

type fieldPathNode struct {
	children map[string]fieldPathNode
}

func newFieldPathNode() fieldPathNode {
	return fieldPathNode{
		children: make(map[string]fieldPathNode),
	}
}

func (p *Plugin) traverseFieldsTree(fpNode fieldPathNode, eventNode *insaneJSON.Node, depth int) bool {
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
	if depth == 0 || shouldPreserveNode {
		// remove all unnecessary fields from current node, if the current node should be preserved
		for _, field := range p.fieldsDepthSlice[depth] {
			eventNode.Dig(field).Suicide()
		}
	}

	p.fieldsDepthSlice[depth] = p.fieldsDepthSlice[depth][:0]

	return shouldPreserveNode
}
