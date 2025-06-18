package metadata

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestTemplaterRender(t *testing.T) {
	tests := []struct {
		name      string
		templates cfg.MetaTemplates
		data      map[string]any
		expected  map[string]any
	}{
		{
			name: "Basic test with single value",
			templates: cfg.MetaTemplates{
				"topic": "{{ .topic }}",
			},
			data: map[string]any{
				"topic": "topic",
			},
			expected: map[string]any{
				"topic": "topic",
			},
		},
		{
			name: "Basic test with template",
			templates: cfg.MetaTemplates{
				"topic": "topic_{{ .topic }}",
			},
			data: map[string]any{
				"topic": "topic",
			},
			expected: map[string]any{
				"topic": "topic_topic",
			},
		},
		{
			name: "Reuse value",
			templates: cfg.MetaTemplates{
				"topic1": "{{ .topic }}",
				"topic2": "{{ .topic1 }}",
			},
			data: map[string]any{
				"topic": "topic",
			},
			expected: map[string]any{
				"topic1": "topic",
				"topic2": "topic",
			},
		},
		{
			name: "With default value",
			templates: cfg.MetaTemplates{
				"topic": `{{ .topic  | default "default_topic" }}`,
			},
			data: map[string]any{
				"topic": "topic",
			},
			expected: map[string]any{
				"topic": "topic",
			},
		},
		{
			name: "Hold values",
			templates: cfg.MetaTemplates{
				"partition_name":      "partition_{{ .partition }}",
				"partition_fullname":  "partition {{ .partition_name }}, topic: {{ .topic }}",
				"partition_fullname2": "{{ .partition_fullname }}",
			},
			data: map[string]any{
				"topic":     "topic",
				"partition": 1,
			},
			expected: map[string]any{
				"partition_name":      "partition_1",
				"partition_fullname":  "partition partition_1, topic: topic",
				"partition_fullname2": "partition partition_1, topic: topic",
			},
		},
		{
			name: "No value",
			templates: cfg.MetaTemplates{
				"header":  `{{ index .headers 0 }}`,
				"header2": "{{ .header }}", // cause we have no {{ .header }}
				"user":    "{{ .auth.user }}",
			},
			data: map[string]any{
				"headers": make(map[string]string),
				"auth":    nil,
			},
			expected: map[string]any{
				"header": "template: :1:3: executing \"\" at <index .headers 0>: error calling index: value has type int; should be string",
				"user":   "template: :1:8: executing \"\" at <.auth.user>: nil pointer evaluating interface {}.user",
			},
		},
		{
			name: "Default values",
			templates: cfg.MetaTemplates{
				"broker_header_default": `{{ index .headers "key" | default "localhost:9093" }}`,

				"broker_name":     "{{ .broker }}",
				"broker_fullname": "{{ .broker_name }}",

				"broker_header": `{{ index .headers "key" | default .broker_fullname }}`,

				"user": "{{ if .auth }}{{ .auth.user | default \"anonymous\" }}{{ else }}{{ \"anonymous\" }}{{ end }}",
			},
			data: map[string]any{
				"headers": make(map[string]string),
				"broker":  "kafka1:9093",
				"auth":    nil,
			},
			expected: map[string]any{
				"broker_header_default": "localhost:9093", // ho value from header, we get default
				"broker_name":           "kafka1:9093",
				"broker_fullname":       "kafka1:9093",
				"broker_header":         "kafka1:9093", // ho value from header, we get from another holded field
				"user":                  "anonymous",
			},
		},
		{
			name: "Nested fields",
			templates: cfg.MetaTemplates{
				"broker_header": `{{ index .headers.broker 0 }}`,
				"broker":        "{{ .broker_header }}",
			},
			data: map[string]any{
				"headers": map[string]any{
					"broker": []string{
						"kafka1:9093",
					},
				},
			},
			expected: map[string]any{
				"broker_header": "kafka1:9093",
				"broker":        "kafka1:9093",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			templater := NewMetaTemplater(
				tt.templates,
				zap.NewExample(),
				32,
			)
			result, err := templater.Render(testMetadata{data: tt.data})
			assert.Nil(t, err)
			assert.Equal(t, fmt.Sprint(tt.expected), fmt.Sprint(result))
		})
	}
}

type testMetadata struct {
	data map[string]any
}

func (f testMetadata) GetData() map[string]any {
	return f.data
}

func BenchmarkMetaTemplater_Render(b *testing.B) {
	templater := NewMetaTemplater(
		cfg.MetaTemplates{
			"broker_header_default": `{{ index .headers "key" | default "localhost:9093" }}`,

			"broker_name":     "{{ .broker }}",
			"broker_fullname": "{{ .broker_name }}",

			"broker_header": `{{ index .headers "key" | default .broker_fullname }}`,

			"user": "{{ if .auth }}{{ .auth.user | default \"anonymous\" }}{{ else }}{{ \"anonymous\" }}{{ end }}",
		}, zap.NewExample(),
		32,
	)

	mockData := map[string]any{
		"headers": make(map[string]string),
		"broker":  "kafka1:9093",
		"auth":    nil,
	}

	for i := 0; i < b.N; i++ {
		_, err := templater.Render(testMetadata{data: mockData})
		if err != nil {
			b.Fatalf("Render failed: %v", err)
		}
	}
}

func TestGenerateCacheKey(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]any
		expected string
	}{
		{
			name:     "empty map",
			input:    map[string]any{},
			expected: "",
		},
		{
			name: "string and int",
			input: map[string]any{
				"topic":     "topic1",
				"partition": 2,
			},
			expected: "topic:topic1|partition:2",
		},
		{
			name: "int32 && int64",
			input: map[string]any{
				"topic":     "topic1",
				"partition": int32(2),
				"offset":    int64(123456789),
			},
			expected: "topic:topic1|partition:2|offset:123456789",
		},
		{
			name: "float",
			input: map[string]any{
				"size": float32(2.0),
			},
			expected: "size:2.000000",
		},
		{
			name: "bool",
			input: map[string]any{
				"is": true,
			},
			expected: "is:true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := generateCacheKey(tt.input)
			if got != tt.expected {
				t.Errorf("generateCacheKey() = %v, want %v", got, tt.expected)
			}
		})
	}
}
