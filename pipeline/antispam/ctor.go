package antispam

import (
	"fmt"

	"github.com/bitly/go-simplejson"
)

func extractNode(jsonNode *simplejson.Json) (Node, error) {
	switch op := jsonNode.Get("op").MustString(); op {
	case "and", "or", "not":
		return extractLogicalNode(op, jsonNode)
	case
		"equal",
		"contains",
		"prefix",
		"suffix",
		"regex":
		return extractValueNode(op, jsonNode)
	default:
		return nil, fmt.Errorf("unknown op: %s", op)
	}
}

func extractLogicalNode(op string, jsonNode *simplejson.Json) (Node, error) {
	rawOperands := jsonNode.Get("operands")

	var operands []Node
	for i := range rawOperands.MustArray() {
		opNode := rawOperands.GetIndex(i)
		operand, err := extractNode(opNode)
		if err != nil {
			return nil, fmt.Errorf("extract operand for logical op %q: %w", op, err)
		}
		operands = append(operands, operand)
	}

	result, err := newLogicalNode(op, operands)
	if err != nil {
		return nil, fmt.Errorf("init logical node: %w", err)
	}

	return result, nil
}

func extractValueNode(op string, jsonNode *simplejson.Json) (Node, error) {
	caseSensitive := jsonNode.Get("case_sensitive").MustBool(true)
	checkDataTag := jsonNode.Get("data").MustString()
	metaKey := jsonNode.Get("meta_key").MustString()

	result, err := newValueNode(op, caseSensitive, nil, checkDataTag, metaKey)
	if err != nil {
		return nil, fmt.Errorf("init value node: %w", err)
	}

	return result, nil
}
