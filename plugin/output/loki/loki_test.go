package loki

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPluginLabels(t *testing.T) {
	type testCase struct {
		name        string
		expectedLen int
		lables      []Lable
	}

	tests := []testCase{
		{
			name:        "one label",
			expectedLen: 1,
			lables: []Lable{
				{
					Label: "label1",
					Value: "value1",
				},
			},
		},
		{
			name:        "two labels",
			expectedLen: 2,
			lables: []Lable{
				{
					Label: "label1",
					Value: "value1",
				},
				{
					Label: "label2",
					Value: "value2",
				},
			},
		},
		{
			name:        "two lables; repetetive label key",
			expectedLen: 1,
			lables: []Lable{
				{
					Label: "label1",
					Value: "value1",
				},
				{
					Label: "label1",
					Value: "value2",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &Plugin{
				config: &Config{
					Labels: tt.lables,
				},
			}

			resultLabelsMap := pl.labels()
			require.Len(t, resultLabelsMap, tt.expectedLen)
		})
	}

}
