package do_if

import (
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/stretchr/testify/require"
)

func TestRuleToNode(t *testing.T) {
	for _, rule := range genAllRules() {
		nRaw, err := RuleToNode(rule, DataTypeEventTag)
		require.NoError(t, err)

		if rule.Invert {
			nLogic := nRaw.(*logicalNode)
			require.Equal(t, nLogic.op, logic.Not)
			require.Equal(t, len(nLogic.operands), 1)

			nRaw = nLogic.operands[0]
		}

		nStr := nRaw.(*stringOpNode)
		require.Equal(t, nStr.dataType, dataTypeEvent)

		c := nStr.checker
		require.Equal(t, c.MinValLen, rule.GetMinValueSize())
		require.Equal(t, c.MinValLen, rule.GetMinValueSize())
		require.True(t, c.ValuesBySize == nil)
		require.Equal(t, c.Op.String(), matchrule.ModeToString(rule.Mode))
		require.Equal(t, c.CaseSensitive, !rule.CaseInsensitive)
		require.Equal(t, c.Values, arrStringToArrBytes(rule.Values))
	}
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
					rule.Prepare()
					rules = append(rules, rule)
				}
			}
		}
	}

	return rules
}
