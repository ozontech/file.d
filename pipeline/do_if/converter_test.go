package do_if

import (
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/stretchr/testify/require"
)

func TestRuleSetToNode(t *testing.T) {
	ruleSets := []matchrule.RuleSet{
		{
			Name:  "sample_or",
			Cond:  matchrule.CondOr,
			Rules: genAllRules(),
		},
		{
			Name:  "sample_and",
			Cond:  matchrule.CondAnd,
			Rules: genAllRules(),
		},
	}

	for _, ruleSet := range ruleSets {
		ruleSet.Prepare()

		rawNode, err := RuleSetToNode(ruleSet, DataTypeEventTag)
		require.NoError(t, err)

		logicNode := rawNode.(*logicalNode)
		require.Equal(t, logicNode.op.String(), ruleSet.Cond.ToString())
		require.Equal(t, len(logicNode.operands), len(ruleSet.Rules))

		for i := range len(logicNode.operands) {
			cmpRuleAndNode(t, ruleSet.Rules[i], logicNode.operands[i])
		}
	}
}

func TestRuleToNode(t *testing.T) {
	for _, rule := range genAllRules() {
		rule.Prepare()

		node, err := RuleToNode(rule, DataTypeEventTag)
		require.NoError(t, err)

		cmpRuleAndNode(t, rule, node)
	}
}

func cmpRuleAndNode(t *testing.T, rule matchrule.Rule, node Node) {
	if rule.Invert {
		nLogic := node.(*logicalNode)
		require.Equal(t, nLogic.op, logic.Not)
		require.Equal(t, len(nLogic.operands), 1)

		node = nLogic.operands[0]
	}

	nStr := node.(*stringOpNode)
	require.Equal(t, nStr.dataType, dataTypeEvent)

	c := nStr.checker
	require.Equal(t, c.MinValLen, rule.GetMinValueSize())
	require.Equal(t, c.MinValLen, rule.GetMinValueSize())
	require.True(t, c.ValuesBySize == nil)
	require.Equal(t, c.Op.String(), rule.Mode.ToString())
	require.Equal(t, c.CaseSensitive, !rule.CaseInsensitive)
	require.Equal(t, c.Values, arrStringToArrBytes(rule.Values))
}

func genAllRules() []matchrule.Rule {
	arrValues := [][]string{
		{"val1", "val2", "val3", "val4"},
		{"a", "bb", "ccc"},
	}
	modes := []matchrule.Mode{
		matchrule.ModePrefix,
		matchrule.ModeContains,
		matchrule.ModeSuffix,
	}
	boolVals := []bool{false, true}

	var rules []matchrule.Rule

	for _, values := range arrValues {
		for _, mode := range modes {
			for _, caseSensitive := range boolVals {
				for _, invert := range boolVals {
					rule := matchrule.Rule{
						Values:          values,
						Mode:            mode,
						CaseInsensitive: caseSensitive,
						Invert:          invert,
					}
					rules = append(rules, rule)
				}
			}
		}
	}

	return rules
}
