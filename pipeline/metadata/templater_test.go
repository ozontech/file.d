package metadata

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/assert"
)

func TestTemplaterRender(t *testing.T) {
	templater := NewMetaTemplater(
		cfg.MetaTemplates{
			"broker_name2":        "{{ .broker_name }}",
			"broker_name":         "{{ .broker }}",
			"partition_fullname2": "{{ .partition_fullname }}",
			"partition_name":      "partition_{{ .partition }}",
			"partition_fullname":  "partition {{ .partition_name }}, topic: {{ .topic }}",
			"topic":               "{{ .topic }}",
			"header":              `{{ index .headers 0 }}`,
			"header_fallback":     `{{ index .headers "key" | default "default_value" }}`,
			"header2":             "{{ .header }}",
			"header3":             `{{ index .headers "key" | default .broker_name2 }}`,
		},
	)

	data := testMetadata{}
	result, err := templater.Render(data)
	assert.Nil(t, err)
	assert.Equal(
		t,
		fmt.Sprint(map[string]any{
			"broker_name":         "kafka1:9093",
			"broker_name2":        "kafka1:9093",
			"partition_name":      "partition_1",
			"partition_fullname":  "partition partition_1, topic: topic",
			"partition_fullname2": "partition partition_1, topic: topic",
			"header":              "template: :1:3: executing \"\" at <index .headers 0>: error calling index: value has type int; should be string",
			"header_fallback":     "default_value",
			"header3":             "kafka1:9093",
			"topic":               "topic",
		}),
		fmt.Sprint(result),
	)
}

type testMetadata struct{}

func (f testMetadata) GetData() map[string]any {
	return map[string]any{
		"topic":     "topic",
		"partition": 1,
		"offset":    1000,
		"broker":    "kafka1:9093",
		"headers":   make(map[string]string),
	}
}
