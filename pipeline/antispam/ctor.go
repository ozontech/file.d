package antispam

import (
	"errors"
	"fmt"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline/ctor"
	"github.com/ozontech/file.d/pipeline/do_if/data_checker"
)

const (
	fieldNameRules     = "rules"
	fieldNameName      = "name"
	fieldNameIf        = "if"
	fieldNameThreshold = "threshold"

	fieldNameOp = "op"

	fieldNameOperands = "operands"

	fieldNameData          = "data"
	fieldNameValues        = "values"
	fieldNameCaseSensitive = "case_sensitive"
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
	name := ""
	nameNode, err := ctor.Get[string](node, fieldNameName)
	if err == nil {
		name = nameNode
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return Rule{}, err
	}

	condNode, err := ctor.Get[map[string]any](node, fieldNameIf)
	if err != nil {
		return Rule{}, err
	}

	cond, err := ctor.Extract(condNode, opToNonLogicalCtor, newLogicalNode)
	if err != nil {
		return Rule{}, err
	}

	thresholdRaw, err := ctor.GetAny(node, fieldNameThreshold)
	if err != nil {
		return Rule{}, err
	}

	threshold, err := cfg.AnyToInt(thresholdRaw)
	if err != nil {
		return Rule{}, err
	}

	return newRule(name, cond, threshold)
}

func opToNonLogicalCtor(opName string) func(string, map[string]any) (Node, error) {
	switch opName {
	case
		data_checker.OpEqualTag,
		data_checker.OpContainsTag,
		data_checker.OpPrefixTag,
		data_checker.OpSuffixTag,
		data_checker.OpRegexTag:
		return extractValueNode
	default:
		return nil
	}
}

func extractValueNode(op string, node map[string]any) (Node, error) {
	dataTag, err := ctor.Get[string](node, fieldNameData)
	if err != nil {
		return nil, err
	}

	caseSensitive := true
	caseSensitiveNode, err := ctor.Get[bool](node, fieldNameCaseSensitive)
	if err == nil {
		caseSensitive = caseSensitiveNode
	} else if errors.Is(err, ctor.ErrTypeMismatch) {
		return nil, err
	}

	values, err := extractValues(node)
	if err != nil {
		return nil, fmt.Errorf("extract values: %w", err)
	}

	return newValueNode(op, caseSensitive, values, dataTag)
}

func extractValues(node map[string]any) ([][]byte, error) {
	rawValues, err := ctor.Get[[]any](node, fieldNameValues)
	if err != nil {
		return nil, err
	}

	values := make([][]byte, 0, len(rawValues))
	for _, rawValue := range rawValues {
		var value string
		value, err = ctor.Must[string](rawValue)
		if err != nil {
			return nil, fmt.Errorf("value type mismatch: %w", err)
		}

		values = append(values, []byte(strings.Clone(value)))
	}

	return values, nil
}
