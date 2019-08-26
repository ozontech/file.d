package filed

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func NewTestConfig(name string) (*Config) {
	return NewConfigFromFile("../testdata/config/" + name)
}

func TestSimple(t *testing.T) {
	c := NewTestConfig("simple.yaml")

	assert.NotNil(t, c, "config loading should't return nil")

	assert.Equal(t, 0, len(c.actions), "actions count isn't match")
	assert.Equal(t, 1, len(c.pipelines), "pipelines count isn't match")
}
