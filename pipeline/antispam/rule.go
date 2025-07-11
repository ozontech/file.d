package antispam

import "fmt"

type Rule struct {
	Condition Node
	Limit     int
	MapKey    string
}

func (r *Rule) Prepare(id int) {
	r.MapKey = fmt.Sprintf("#=%d=#", id)
}

func checkLimit(limit int) error {
	if limit < -1 {
		return fmt.Errorf("invalid limit: %d", limit)
	}
	return nil
}

func newRule(condition Node, limit int) (Rule, error) {
	if err := checkLimit(limit); err != nil {
		return Rule{}, err
	}

	return Rule{
		Condition: condition,
		Limit:     limit,
	}, nil
}
