package antispam

import "fmt"

type Rule struct {
	Condition Node
	Threshold int
	RLMapKey  string
}

func (r *Rule) Prepare(id int) {
	r.RLMapKey = fmt.Sprintf("#=%d=#", id)
}

func checkThreshold(threshold int) error {
	if threshold < -1 {
		return fmt.Errorf("invalid threshold: %d", threshold)
	}

	return nil
}

func newRule(condition Node, threshold int) (Rule, error) {
	if err := checkThreshold(threshold); err != nil {
		return Rule{}, err
	}

	return Rule{
		Condition: condition,
		Threshold: threshold,
	}, nil
}
