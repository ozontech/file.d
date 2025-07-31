package str_checker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckerCtor(t *testing.T) {
	type testCase struct {
		opTag         string
		caseSensitive bool
		values        [][]byte

		expected DataChecker
	}

	for _, tt := range []testCase{
		{
			opTag:         OpEqualTag,
			caseSensitive: true,
			values:        [][]byte{[]byte(`test-111`), []byte(`test-2`), []byte(`test-3`), []byte(`test-12345`)},

			expected: DataChecker{
				Op:            OpEqual,
				CaseSensitive: true,
				Values:        nil,
				ValuesBySize: map[int][][]byte{
					6: {
						[]byte(`test-2`),
						[]byte(`test-3`),
					},
					8: {
						[]byte(`test-111`),
					},
					10: {
						[]byte(`test-12345`),
					},
				},
				ReValues:  nil,
				MinValLen: 6,
				MaxValLen: 10,
			},
		},
		{
			opTag:         OpContainsTag,
			caseSensitive: false,
			values: [][]byte{
				[]byte(`test-0987`),
				[]byte(`test-11`),
			},

			expected: DataChecker{
				Op:            OpContains,
				CaseSensitive: false,
				Values: [][]byte{
					[]byte(`test-0987`),
					[]byte(`test-11`),
				},
				ValuesBySize: nil,
				ReValues:     nil,
				MinValLen:    7,
				MaxValLen:    9,
			},
		},
	} {
		got, err := New(tt.opTag, tt.caseSensitive, tt.values)
		require.NoErrorf(t, err, "failed to init checker")
		require.NoError(t, Equal(&got, &tt.expected), "checkers are not equal")
	}
}
