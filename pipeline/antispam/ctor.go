package antispam

import (
	"errors"
	"fmt"

	"github.com/bitly/go-simplejson"
)

func extractRules(jsonNode *simplejson.Json) ([]Rule, error) {
	rules := jsonNode.Get("rules")

	var result []Rule
	for i := range rules.MustArray() {
		ruleRaw := rules.GetIndex(i)
		rule, err := extractRule(ruleRaw)
		if err != nil {
			return nil, fmt.Errorf("extract rule: %w", err)
		}

		result = append(result, rule)
	}

	return result, nil
}

func extractRule(jsonNode *simplejson.Json) (Rule, error) {
	condition, err := extractNode(jsonNode.Get("cond"))
	if err != nil {
		return Rule{}, fmt.Errorf("extract cond: %w", err)
	}

	limit, err := jsonNode.Get("limit").Int()
	if err != nil {
		return Rule{}, fmt.Errorf("limit is not int: %w", err)
	}

	return newRule(condition, limit)
}

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

	values, err := extractFieldOpVals(jsonNode)
	if err != nil {
		return nil, fmt.Errorf("extract values: %w", err)
	}

	result, err := newValueNode(op, caseSensitive, values, checkDataTag, metaKey)
	if err != nil {
		return nil, fmt.Errorf("init value node: %w", err)
	}

	return result, nil
}

func extractFieldOpVals(jsonNode *simplejson.Json) ([][]byte, error) {
	values, has := jsonNode.CheckGet("values")
	if !has {
		return nil, errors.New(`field "values" not found'`)
	}

	var result [][]byte
	for i := range values.MustArray() {
		curValue, err := values.GetIndex(i).String()
		if err != nil {
			return nil, err
		}

		result = append(result, []byte(curValue))
	}

	return result, nil
}
