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
			"partition_name":     "partition_{{ .partition }}",
			"partition_fullname": "partition {{ .partition_name }}",
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
			"partition_name":     "partition_1",
			"partition_fullname": "partition partition_1",
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
