package fd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func NewTestConfig(name string) *Config {
	return NewConfigFromFile("../testdata/config/" + name)
}

func TestSimple(t *testing.T) {
	c := NewTestConfig("e2e.yaml")

	assert.NotNil(t, c, "config loading should't return nil")

	assert.Equal(t, 1, len(c.Pipelines), "pipelines count isn't match")
}

type strRequired struct {
	T string
}

type strDefault struct {
	T string `default:"sync"`
}

type strDuration struct {
	T  Duration `default:"5s" parse:"duration"`
	T_ time.Duration
}

type strOptions struct {
	T string `default:"async" options:"async|sync"`
}

func TestParseRequiredOk(t *testing.T) {
	s := &strRequired{T: "some_value"}
	err := Parse(s)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseRequiredFail(t *testing.T) {
	s := &strRequired{}
	err := Parse(s)

	assert.NotNil(t, err, "should be an error")
}

func TestParseDefault(t *testing.T) {
	s := &strDefault{}
	err := Parse(s)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "sync", s.T, "wrong value")
}

func TestParseDuration(t *testing.T) {
	s := &strDuration{}
	err := Parse(s)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, time.Second*5, s.T_, "wrong value")
}

func TestParseOptionsOk(t *testing.T) {
	s := &strOptions{T: "async"}
	err := Parse(s)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseOptionsFail(t *testing.T) {
	s := &strOptions{T: "sequential"}
	err := Parse(s)

	assert.NotNil(t, err, "should be an error")
}
