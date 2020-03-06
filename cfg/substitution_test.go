package cfg

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseVale(t *testing.T) {
	result, err := ParseSubstitution("just value")

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 1, len(result), "wrong result")
	assert.Equal(t, "just value", result[0].Data[0], "wrong result")
}

func TestParseField(t *testing.T) {
	result, err := ParseSubstitution("days till world end ${prediction.days}. so what?")

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 3, len(result), "wrong result")
	assert.Equal(t, "days till world end ", result[0].Data[0], "wrong result")
	assert.Equal(t, "prediction", result[1].Data[0], "wrong result")
	assert.Equal(t, "days", result[1].Data[1], "wrong result")
	assert.Equal(t, ". so what?", result[2].Data[0], "wrong result")
}

func TestParseEnding(t *testing.T) {
	result, err := ParseSubstitution("days till world end ${prediction.days}")

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 2, len(result), "wrong result")
	assert.Equal(t, "days till world end ", result[0].Data[0], "wrong result")
	assert.Equal(t, "prediction", result[1].Data[0], "wrong result")
	assert.Equal(t, "days", result[1].Data[1], "wrong result")
}

func TestParseEscape(t *testing.T) {
	result, err := ParseSubstitution("days till world end $$100")

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 1, len(result), "wrong result")
	assert.Equal(t, "days till world end $100", result[0].Data[0], "wrong result")
}

func TestParseErr(t *testing.T) {
	_, err := ParseSubstitution("days till world end ${prediction.days. so what?")

	assert.NotNil(t, err, "no error")
}
