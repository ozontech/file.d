package config

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func NewTestConfig(name string) (*Config, error) {
	return NewConfigFromFile("../testdata/config/" + name)
}

func TestSimple(t *testing.T) {
	c, err := NewTestConfig("simple.yaml")

	assert.NoError(t, err, "Config loading shouldn't cause error")
	assert.NotNil(t, c, "Config loading should't return nil")

	assert.Equal(t, 2, len(c.inputs), "Inputs count isn't match")
	assert.Equal(t, 2, len(c.processors), "Processors count isn't match")
	assert.Equal(t, 2, len(c.outputs), "Outputs count isn't match")
	assert.Equal(t, 2, len(c.pipelines), "Pipelines count isn't match")
}
