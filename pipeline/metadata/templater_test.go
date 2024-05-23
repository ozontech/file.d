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
			"partition":          "partition_{{ .partition }}",
			"partition_describe": "{{ .partition }} partition",
			"topic":              "{{ .topic }}",
			"broker":             "{{ .broker }}",
		},
	)

	data := testMetadata{}
	result, err := templater.Render(data)
	assert.Nil(t, err)
	assert.Equal(
		t,
		fmt.Sprint(map[string]any{
			"topic":              "topic",
			"partition":          "partition_1",
			"partition_describe": "1 partition",
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
	}
}
