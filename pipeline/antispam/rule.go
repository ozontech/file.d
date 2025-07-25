package antispam

import (
	"fmt"

	"github.com/ozontech/file.d/pipeline/do_if"
)

type Rule struct {
	Name      string
	Condition do_if.Node
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

func newRule(name string, condition do_if.Node, threshold int) (Rule, error) {
	if err := checkThreshold(threshold); err != nil {
		return Rule{}, err
	}

	return Rule{
		Name:      name,
		Condition: condition,
		Threshold: threshold,
	}, nil
}

func exceptionToNode(exception Exception) (do_if.Node, error) {
	dataTypeTag := do_if.DataTypeEventTag
	if exception.CheckSourceName {
		dataTypeTag = do_if.DataTypeSourceNameTag
	}

	return do_if.RuleSetToNode(exception.RuleSet, dataTypeTag)
}

func exceptionsToRules(exceptions Exceptions) (Rules, error) {
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
