package antispam

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline/ctor"
	"github.com/ozontech/file.d/pipeline/do_if"
)

const (
	fieldNameRules = "rules"

	fieldNameThreshold = "threshold"

	fieldNameName         = "name"
	fieldNameIf           = "if"
	fieldNameUniteSources = "unite_sources"
)

func extractAntispam(node map[string]any) ([]Rule, int, error) {
	thresholdNode, err := ctor.GetAny(node, fieldNameThreshold)
	if err != nil {
		return nil, 0, err
	}

	threshold, err := cfg.AnyToInt(thresholdNode)
	if err != nil {
		return nil, 0, err
	}

	var rules []Rule

	rawRules, err := ctor.Get[[]any](node, fieldNameRules)
	if err == nil {
		rules, err = extractRules(rawRules)
		if err == nil {
			return nil, 0, err
		}
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, 0, err
	}

	return rules, threshold, nil
}

func extractRules(rawRules []any) ([]Rule, error) {
	rules := make([]Rule, 0, len(rawRules))

	for _, rawRule := range rawRules {
		ruleNode, err := ctor.Must[map[string]any](rawRule)
		if err != nil {
			return nil, fmt.Errorf("rule type mismatch: %w", err)
		}

		rule, err := extractRule(ruleNode)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	return rules, nil
}

func extractRule(node map[string]any) (Rule, error) {
	def := Rule{}

	name, err := ctor.Get[string](node, fieldNameName, "")
	if err != nil {
		return def, err
	}

	uniteSources, err := ctor.Get[bool](node, fieldNameUniteSources, false)
	if err != nil {
		return def, err
	}

	condNode, err := ctor.Get[map[string]any](node, fieldNameIf)
	if err != nil {
		return def, err
	}

	cond, err := do_if.ExtractNode(condNode)
	if err != nil {
		return def, err
	}

	thresholdRaw, err := ctor.GetAny(node, fieldNameThreshold)
	if err != nil {
		return def, err
	}

	threshold, err := cfg.AnyToInt(thresholdRaw)
	if err != nil {
		return def, err
	}

	return newRule(name, cond, threshold, uniteSources)
}
