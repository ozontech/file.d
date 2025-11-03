package loki

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPluginParseLabels(t *testing.T) {
	type testCase struct {
		name        string
		expectedLen int
		lables      []Label
	}

	tests := []testCase{
		{
			name:        "one label",
			expectedLen: 1,
			lables: []Label{
				{
					Label: "label1",
					Value: "value1",
				},
			},
		},
		{
			name:        "two labels",
			expectedLen: 2,
			lables: []Label{
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
			lables: []Label{
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

			resultLabelsMap := pl.parseLabels()
			require.Len(t, resultLabelsMap, tt.expectedLen)
		})
	}
}

func TestPluginLabelsString(t *testing.T) {
	type testCase struct {
		name                string
		expectedLabelsCount int
		labels              Labels
	}

	tests := []testCase{
		{
			name:                "one label",
			expectedLabelsCount: 1,
			labels: Labels(map[string]string{
				"a": "1",
			}),
		},
		{
			name:                "two labels; ascending order",
			expectedLabelsCount: 2,
			labels: Labels(map[string]string{
				"a": "1",
				"b": "2",
			}),
		},
		{
			name:                "two labels; descending order",
			expectedLabelsCount: 4,
			labels: Labels(map[string]string{
				"b": "2",
				"a": "1",
				"c": "3",
				"d": "4",
			}),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.expectedLabelsCount, len(strings.Split(tt.labels.String(), ", ")))
		})
	}
}

func TestPluginGetAuthHeaders(t *testing.T) {
	type testCase struct {
		name     string
		strategy string

		username    string
		password    string
		expectBasic bool

		bearer       string
		expectBearer bool
	}

	tests := []testCase{
		{
			name:        "only basic",
			username:    "username",
			password:    "password",
			strategy:    StrategyBasic,
			expectBasic: true,
		},
		{
			name:         "bearer only",
			bearer:       "token",
			strategy:     StrategyBearer,
			expectBearer: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &Plugin{
				config: &Config{
					Auth: AuthConfig{
						Strategy:    tt.strategy,
						Username:    tt.username,
						Password:    tt.password,
						BearerToken: tt.bearer,
					},
				},
			}

			header := pl.getAuthHeader()
			if tt.expectBasic {
				credentials := []byte(pl.config.Auth.Username + ":" + pl.config.Auth.Password)
				require.Equal(t, fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString(credentials)), header)
			}

			if tt.expectBearer {
				require.Equal(t, fmt.Sprintf("Bearer %s", pl.config.Auth.BearerToken), header)
			}
		})
	}
}

func TestPluginGetCustomHeaders(t *testing.T) {
	type testCase struct {
		name         string
		strategy     string
		tenantID     string
		expectTenant bool
	}

	tests := []testCase{
		{
			name:         "only tenant",
			tenantID:     "tenant",
			strategy:     StrategyTenant,
			expectTenant: true,
		},
		{
			name:     "without headers",
			tenantID: "tenant",
			strategy: StrategyDisabled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pl := &Plugin{
				config: &Config{
					Auth: AuthConfig{
						Strategy: tt.strategy,
						TenantID: tt.tenantID,
					},
				},
			}

			headers := pl.getCustomHeaders()

			if tt.expectTenant {
				require.Len(t, headers, 1)
				require.Equal(t, tt.tenantID, headers["X-Scope-OrgID"])
			} else {
				require.Len(t, headers, 0)
			}
		})
	}
}

func TestIsUnixNanoFormat(t *testing.T) {
	type args struct {
		name         string
		ts           string
		expectResult bool
	}

	testCases := []args{
		{
			name:         "valid",
			ts:           "1700000000000000000",
			expectResult: true,
		},
		{
			name:         "Too Large",
			ts:           "9999999999999999999", // 20 November 2286
			expectResult: false,
		},
		{
			name:         "Valid Timestamp",
			ts:           "1609459200123456789", // 1 January 2021
			expectResult: true,
		},
		{
			name:         "Invalid Non-Numeric",
			ts:           "hello123456789",
			expectResult: false,
		},
		{
			name:         "now",
			ts:           fmt.Sprintf("%d", time.Now().UnixNano()),
			expectResult: true,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			p := &Plugin{}

			result := p.isUnixNanoFormat(tt.ts)
			if result != tt.expectResult {
				t.Errorf("isUnixNano(%s) = %v, want %v", tt.ts, result, tt.expectResult)
			}
		})
	}
}
