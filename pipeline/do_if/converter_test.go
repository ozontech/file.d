package do_if

import (
	"testing"

	"github.com/ozontech/file.d/cfg/matchrule"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/stretchr/testify/require"
)

func TestRuleToNode(t *testing.T) {
	rule := matchrule.Rule{
		Values:          []string{"a", "bb", "ccc"},
		Mode:            matchrule.ModeSuffix,
		CaseInsensitive: true,
		Invert:          true,
	}

	rule.Prepare()

	nRaw, err := RuleToNode(rule, DataTypeEventTag)
	require.NoError(t, err)

	nLogic := nRaw.(*logicalNode)
	require.Equal(t, nLogic.op, logic.Not)
	require.Equal(t, len(nLogic.operands), 1)

	nOperand := nLogic.operands[0].(*stringOpNode)
	require.Equal(t, nOperand.dataType, dataTypeEvent)

	c := nOperand.checker
	require.Equal(t, c.MinValLen, rule.GetMinValueSize())
	require.Equal(t, c.MinValLen, rule.GetMinValueSize())
	require.True(t, c.ValuesBySize == nil)
	require.True(t, c.Values == nil)
	require.Equal(t, c.Op.String(), matchrule.ModeToString(rule.Mode))
	require.Equal(t, c.CaseSensitive, !rule.CaseInsensitive)
	require.Equal(t, c.Values, [][]byte{[]byte("a"), []byte("bb"), []byte("ccc")})
}
