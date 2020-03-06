package cfg

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
	T string `required:"true"`
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

type strExpression struct {
	T  string `parse:"expression"`
	T_ int
}

func TestParseRequiredOk(t *testing.T) {
	s := &strRequired{T: "some_value"}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseRequiredErr(t *testing.T) {
	s := &strRequired{}
	err := Parse(s, nil)

	assert.NotNil(t, err, "should be an error")
}

func TestParseDefault(t *testing.T) {
	s := &strDefault{}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, "sync", s.T, "wrong value")
}

func TestParseDuration(t *testing.T) {
	s := &strDuration{}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
	assert.Equal(t, time.Second*5, s.T_, "wrong value")
}

func TestParseOptionsOk(t *testing.T) {
	s := &strOptions{T: "async"}
	err := Parse(s, nil)

	assert.NoError(t, err, "shouldn't be an error")
}

func TestParseOptionsErr(t *testing.T) {
	s := &strOptions{T: "sequential"}
	err := Parse(s, nil)

	assert.NotNil(t, err, "should be an error")
}
func TestParseExpressionMul(t *testing.T) {
	s := &strExpression{T: "val*2"}
	err := Parse(s, map[string]int{"val": 3})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 6, s.T_, "wrong value")
}

func TestParseExpressionAdd(t *testing.T) {
	s := &strExpression{T: "10+val"}
	err := Parse(s, map[string]int{"val": 3})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 13, s.T_, "wrong value")
}

func TestParseExpressionConst(t *testing.T) {
	s := &strExpression{T: "10"}
	err := Parse(s, map[string]int{"val": 12})

	assert.Nil(t, err, "shouldn't be an error")
	assert.Equal(t, 10, s.T_, "wrong value")
}

func TestParseFieldSelectorSimple(t *testing.T) {
	path := ParseFieldSelector("a.b.c")

	assert.Equal(t, 3, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b", path[1], "wrong field")
	assert.Equal(t, "c", path[2], "wrong field")
}

func TestParseFieldSelectorEscape(t *testing.T) {
	path := ParseFieldSelector("a.b..c")

	assert.Equal(t, 2, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b.c", path[1], "wrong field")
}

func TestParseFieldSelectorEnding(t *testing.T) {
	path := ParseFieldSelector("a.b.c..")

	assert.Equal(t, 3, len(path), "wrong length")
	assert.Equal(t, "a", path[0], "wrong field")
	assert.Equal(t, "b", path[1], "wrong field")
	assert.Equal(t, "c.", path[2], "wrong field")
}
