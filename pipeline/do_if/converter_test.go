package do_if

import (
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/stretchr/testify/require"
)

func TestRuleToNode(t *testing.T) {
	rules := []matchrule.Rule{
		{
			Values:          []string{"a", "bb", "ccc"},
			Mode:            matchrule.ModeSuffix,
			CaseInsensitive: true,
			Invert:          true,
		},
		{
			Values:          []string{"a", "bb", "ccc"},
			Mode:            matchrule.ModeSuffix,
			CaseInsensitive: true,
			Invert:          false,
		},
	}

	for _, rule := range rules {
		rule.Prepare()

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
