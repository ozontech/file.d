package antispam

import (
	"fmt"
	"strings"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/ctor"
	"github.com/ozontech/file.d/pipeline/logic"
)

type Rule struct {
	Name      string
	Condition Node
	Threshold int
	RLMapKey  string
}

type Rules []Rule

func (r *Rule) Prepare(id int) {
	r.RLMapKey = fmt.Sprintf("#=%d=#", id)
}

func checkThreshold(threshold int) error {
	if threshold < -1 {
		return fmt.Errorf("invalid threshold: expected non-negative or -1 got=%d", threshold)
	}

	return nil
}

func newRule(name string, condition Node, threshold int) (Rule, error) {
	if err := checkThreshold(threshold); err != nil {
		return Rule{}, err
	}

	return Rule{
		Name:      name,
		Condition: condition,
		Threshold: threshold,
	}, nil
}

func matchRuleToNode(rule matchrule.Rule, dataTypeTag string) (Node, error) {
	values := make([][]byte, 0, len(rule.Values))
	for _, s := range rule.Values {
		values = append(values, []byte(strings.Clone(s)))
	}

	node, err := newValueNode(matchrule.ModeToString(rule.Mode), !rule.CaseInsensitive, values, dataTypeTag)
	if err != nil {
		return nil, err
	}

	if !rule.Invert {
		return node, nil
	}

	return ctor.NewLogicalNode(logic.NotTag, []Node{node}, newLogicalNode)
}

func matchRuleSetToNode(ruleSet matchrule.RuleSet, dataTypeTag string) (Node, error) {
	operands := make([]Node, 0, len(ruleSet.Rules))
	for _, r := range ruleSet.Rules {
		operand, err := matchRuleToNode(r, dataTypeTag)
		if err != nil {
			return nil, err
		}
		operands = append(operands, operand)
	}

	return ctor.NewLogicalNode(matchrule.CondToString(ruleSet.Cond), operands, newLogicalNode)
}

func exceptionToNode(exception Exception) (Node, error) {
	dataTypeTag := dataTypeEventTag
	if exception.CheckSourceName {
		dataTypeTag = dataTypeSourceNameTag
	}

	return matchRuleSetToNode(exception.RuleSet, dataTypeTag)
}

func exceptionToRules(exceptions Exceptions) (Rules, error) {
	rules := make(Rules, 0, len(exceptions))
	for _, e := range exceptions {
		node, err := exceptionToNode(e)
		if err != nil {
			return nil, err
		}

		rule, err := newRule(e.RuleSet.Name, node, -1)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	return rules, nil
}
