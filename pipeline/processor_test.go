package pipeline

import (
	"strconv"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

func Test_processor_isMatch(t *testing.T) {
	type TestCase struct {
		Conds     []MatchCondition
		MatchMode MatchMode
		Log       string
		MustMatch bool
	}

	tcs := []TestCase{
		{
			Conds: []MatchCondition{
				{
					Field:  []string{"k8s_label_app"},
					Values: []string{"address-api", "tarifficator-api"},
				},
				{
					Field:  []string{"ns"},
					Values: []string{"map"},
				},
			},
			MatchMode: MatchModeAnd, Log: `{"k8s_label_app": "address-api", "ns": "map"}`,
			MustMatch: true,
		},
		{
			Conds: []MatchCondition{
				{
					Field:  []string{"k8s_pod"},
					Values: []string{"address-api", "card-api"},
				},
				{
					Field:  []string{"ns"},
					Values: []string{"map"},
				},
			},
			MatchMode: MatchModeAndPrefix, Log: `{"k8s_pod": "address-api-abcd-123123", "ns": "map"}`,
			MustMatch: true,
		},
		{
			Conds: []MatchCondition{
				{
					Field:  []string{"k8s_pod"},
					Values: []string{"address-api", "tarifficator-api", "card-api"},
				},
				{
					Field:  []string{"ns"},
					Values: []string{"map"},
				},
			},
			MatchMode: MatchModeOrPrefix, Log: `{"k8s_pod": "address-api-abcd-123123", "ns": "map"}`,
			MustMatch: true,
		},

		// negative test cases
		{
			Conds: []MatchCondition{
				{
					Field:  []string{"k8s_pod"},
					Values: []string{"address-api-abcd-123123"},
				},
				{
					Field:  []string{"ns"},
					Values: []string{"map"},
				},
			},
			MatchMode: MatchModeAnd, Log: `{"k8s_label_app": "address-api", "ns": "map"}`,
		},
		{
			Conds: []MatchCondition{
				{
					Field:  []string{"k8s_pod"},
					Values: []string{"address-api-abcd-123123"},
				},
				{
					Field:  []string{"ns"},
					Values: []string{"sc"},
				},
			},
			MatchMode: MatchModeOr, Log: `{"k8s_label_app": "address-api", "ns": "map"}`,
		},
	}

	for i, tc := range tcs {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			proc := processor{
				busyActions: []bool{false},
				actionInfos: []*ActionPluginStaticInfo{
					{
						MatchConditions: tc.Conds,
						MatchMode:       tc.MatchMode,
					},
				},
			}

			event := newEvent()
			require.NoError(t, event.Root.DecodeString(tc.Log))

			ok := proc.isMatch(0, event)
			insaneJSON.Release(event.Root)

			require.Equalf(t, tc.MustMatch, ok, "Condition fails with %v for the log %s", tc.Conds, tc.Log)
		})
	}
}
