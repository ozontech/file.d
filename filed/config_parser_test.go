package filed

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewTestConfig(name string) (*Config, error) {
	return NewConfigFromFile("../testdata/config/" + name)
}

func TestSimple(t *testing.T) {
	c, err := NewTestConfig("simple.yaml")

	assert.NoError(t, err, "config loading shouldn't cause error")
	assert.NotNil(t, c, "config loading should't return nil")

	assert.Equal(t, 2, len(c.inputs), "inputs count isn't match")
	assert.Equal(t, 2, len(c.processors), "processors count isn't match")
	assert.Equal(t, 2, len(c.outputs), "outputs count isn't match")
	assert.Equal(t, 2, len(c.pipelines), "pipelines count isn't match")
}
