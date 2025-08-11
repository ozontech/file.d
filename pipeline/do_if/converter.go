package do_if

import (
	"strings"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
)

func RuleToNode(rule matchrule.Rule, dataTypeTag string) (Node, error) {
	values := arrStringToArrBytes(rule.Values)
	node, err := newStringOpNode(
		rule.Mode.ToString(),
		!rule.CaseInsensitive,
		values,
		"",
		dataTypeTag,
	)
	if err != nil {
		return nil, err
	}

	if !rule.Invert {
		return node, nil
	}

	return newLogicalNode(logic.NotTag, []Node{node})
}

func arrStringToArrBytes(a []string) [][]byte {
	res := make([][]byte, 0, len(a))
	for _, s := range a {
		res = append(res, []byte(strings.Clone(s)))
	}

	return res
}

func RuleSetToNode(ruleSet matchrule.RuleSet, dataTypeTag string) (Node, error) {
	operands := make([]Node, 0, len(ruleSet.Rules))
	for _, r := range ruleSet.Rules {
		operand, err := RuleToNode(r, dataTypeTag)
		if err != nil {
			return nil, err
		}
		operands = append(operands, operand)
	}

	return newLogicalNode(ruleSet.Cond.ToString(), operands)
}
