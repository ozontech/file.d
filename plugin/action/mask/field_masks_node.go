package mask

import (
	"fmt"

	"github.com/ozontech/file.d/cfg"
)

// fieldMasksNode is a supplemental data structure for global and mask-specific process/ignore fields lists.
type fieldMasksNode struct {
	processMasks  map[int]struct{}
	ignoreMasks   map[int]struct{}
	globalProcess bool
	globalIgnore  bool
	children      map[string]*fieldMasksNode
}

func newFieldMasksNode() *fieldMasksNode {
	return &fieldMasksNode{
		children: map[string]*fieldMasksNode{},
	}
}

func addFieldsToTree(root *fieldMasksNode, fieldPaths [][]string, cb func(*fieldMasksNode)) {
	for _, fieldPath := range fieldPaths {
		curNode := root
		for j, field := range fieldPath {
			nextNode, has := curNode.children[field]
			if !has {
				nextNode = newFieldMasksNode()
				curNode.children[field] = nextNode
			}
			if j == len(fieldPath)-1 { // leaf node
				cb(nextNode)
			}
			curNode = nextNode
		}
	}
}

func (p *Plugin) gatherFieldMasksTree() error {
	root := newFieldMasksNode()
	if len(p.config.IgnoreFields) > 0 && len(p.config.ProcessFields) > 0 {
		return fmt.Errorf("cannot use ignore fields and process fields simoultaneously")
	}

	p.hasMasksIgnoreFields = make([]bool, len(p.config.Masks))
	p.hasMasksProcessFields = make([]bool, len(p.config.Masks))

	for i := range p.config.Masks {
		_ignoreFields := p.config.Masks[i].IgnoreFields
		_processFields := p.config.Masks[i].ProcessFields
		if len(_ignoreFields) > 0 && len(_processFields) > 0 {
			return fmt.Errorf("cannot use ignore fields and process fields simoultaneously for mask[%d]", i)
		}
		if len(_ignoreFields) > 0 {
			maskIgnoreFields, err := cfg.ParseNestedFields(_ignoreFields)
			if err != nil {
				return err
			}
			addFieldsToTree(root, maskIgnoreFields, func(n *fieldMasksNode) {
				if n.ignoreMasks == nil {
					n.ignoreMasks = make(map[int]struct{})
				}
				n.ignoreMasks[i] = struct{}{}
			})
			p.hasMasksIgnoreFields[i] = true
			p.hasMaskSpecificFieldsList = true
			p.hasProcessOrIgnoreFields = true
		}
		if len(_processFields) > 0 {
			maskProcessFields, err := cfg.ParseNestedFields(_processFields)
			if err != nil {
				return err
			}
			addFieldsToTree(root, maskProcessFields, func(n *fieldMasksNode) {
				if n.processMasks == nil {
					n.processMasks = make(map[int]struct{})
				}
				n.processMasks[i] = struct{}{}
			})
			p.hasMasksProcessFields[i] = true
			p.hasMaskSpecificFieldsList = true
			p.hasProcessOrIgnoreFields = true
		}
	}

	masksWithSpecificFieldsLists := 0
	if p.hasMaskSpecificFieldsList {
		for i := range p.config.Masks {
			if p.hasMasksIgnoreFields[i] || p.hasMasksProcessFields[i] {
				masksWithSpecificFieldsLists++
			}
		}
	}
	// if every mask has specific ignore/process fields lists, no need to apply global ignore/process fields lists
	if masksWithSpecificFieldsLists < len(p.config.Masks) {
		if len(p.config.IgnoreFields) > 0 {
			globalIgnoreFields, err := cfg.ParseNestedFields(p.config.IgnoreFields)
			if err != nil {
				return err
			}
			addFieldsToTree(root, globalIgnoreFields, func(n *fieldMasksNode) {
				n.globalIgnore = true
			})
			p.hasGlobalIgnoreFields = true
			p.hasProcessOrIgnoreFields = true
		}

		if len(p.config.ProcessFields) > 0 {
			globalProcessFields, err := cfg.ParseNestedFields(p.config.ProcessFields)
			if err != nil {
				return err
			}
			addFieldsToTree(root, globalProcessFields, func(n *fieldMasksNode) {
				n.globalProcess = true
			})
			p.hasGlobalProcessFields = true
			p.hasProcessOrIgnoreFields = true
		}
	} else {
		p.logger.Warn("every mask has specific ignore/process fields list, global ignore/process fields list is omitted")
	}

	if p.hasProcessOrIgnoreFields {
		p.fieldMasksRoot = root
		p.emptyFMNode = newFieldMasksNode()
	}

	return nil
}

func (p *Plugin) gatherFieldPaths() error {
	var err error
	var fieldPaths []string

	isBlacklist := len(p.config.IgnoreFields) > 0
	isWhitelist := len(p.config.ProcessFields) > 0
	switch {
	case isBlacklist && isWhitelist:
		return fmt.Errorf("ignored fields list and processed fields lists are both non-empty")
	case isBlacklist:
		fieldPaths = p.config.IgnoreFields
	case isWhitelist:
		fieldPaths = p.config.ProcessFields
	}

	if len(fieldPaths) > 0 {
		p.fieldPaths = make([][]string, 0, len(fieldPaths))
		p.fieldPaths, err = cfg.ParseNestedFields(fieldPaths)
		if err != nil {
			return fmt.Errorf("failed to parse fields: %w", err)
		}
	}

	return nil
}
