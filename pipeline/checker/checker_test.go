package checker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckerCtor(t *testing.T) {
	type testCase struct {
		opTag         string
		caseSensitive bool
		values        [][]byte

		expected *Checker
	}

	for _, tt := range []testCase{
		{
			opTag:         OpEqualTag,
			caseSensitive: true,
			values:        [][]byte{[]byte(`test-111`), []byte(`test-2`), []byte(`test-3`), []byte(`test-12345`)},

			expected: &Checker{
				op:            opEqual,
				caseSensitive: true,
				values:        nil,
				valuesBySize: map[int][][]byte{
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
				reValues:  nil,
				minValLen: 6,
				maxValLen: 10,
			},
		},
		{
			opTag:         OpContainsTag,
			caseSensitive: false,
			values: [][]byte{
				[]byte(`test-0987`),
				[]byte(`test-11`),
			},

			expected: &Checker{
				op:            opContains,
				caseSensitive: false,
				values: [][]byte{
					[]byte(`test-0987`),
					[]byte(`test-11`),
				},
				valuesBySize: nil,
				reValues:     nil,
				minValLen:    7,
				maxValLen:    9,
			},
		},
	} {
		got, err := New(tt.opTag, tt.caseSensitive, tt.values)
		require.NoErrorf(t, err, "failed to init checker")
		require.NoError(t, Equal(got, tt.expected), "checkers are not equal")
	}
}
