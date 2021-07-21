package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ShouldSkip(t *testing.T) {
	testCases := []struct {
		testName        string
		isMatch         bool
		invertMatchMode bool
		expectedResult  bool
	}{
		{
			"ordinary logic",
			true,
			false,
			false,
		},
		{
			"ordinary logic",
			false,
			false,
			true,
		},
		{
			"skip action",
			true,
			true,
			true,
		},
		{"skip action false",
			false,
			true,
			false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			tc := tc
			infos := []*ActionPluginStaticInfo{
				{
					InvertMatchMode: tc.invertMatchMode,
				},
			}
			proc := &processor{actionInfos: infos}
			idx := 0
			res := proc.shouldSkip(idx, tc.isMatch)
			assert.Equal(t, tc.expectedResult, res)
		})
	}
}
