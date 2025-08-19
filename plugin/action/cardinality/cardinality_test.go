package discard

import (
	"sort"
	"strings"
	"testing"
)

func TestMapToStringSorted(t *testing.T) {
	tests := []struct {
		name     string
		m        map[string]string
		n        map[string]string
		expected string
	}{
		{
			name:     "empty maps",
			m:        map[string]string{},
			n:        map[string]string{},
			expected: "map[];map[]",
		},
		{
			name:     "simple maps",
			m:        map[string]string{"service": "test", "host": "localhost"},
			n:        map[string]string{"value": "1", "level": "3"},
			expected: "map[host:localhost service:test];map[level:3 value:1]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mapToStringSorted(tt.m, tt.n)
			if result != tt.expected {
				t.Errorf("mapToStringSorted() = %v, want %v", result, tt.expected)
			}

			// Verify the output is properly sorted
			if strings.HasPrefix(result, "map[") && strings.HasSuffix(result, "]") {
				content := result[4 : len(result)-1]
				if content != "" {
					pairs := strings.Split(content, " ")
					if !sort.StringsAreSorted(pairs) {
						t.Errorf("output pairs are not sorted: %v", pairs)
					}
				}
			}
		})
	}
}
