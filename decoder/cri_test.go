package decoder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestCRIPartial(t *testing.T) {
	root := insaneJSON.Spawn()
	err := DecodeCRI(root, []byte("2016-10-06T00:17:09.669794202Z stdout P partial content 1\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2016-10-06T00:17:09.669794202Z", root.Dig("time").AsString())
	assert.Equal(t, "stdout", root.Dig("stream").AsString())
	assert.Equal(t, "partial content 1", root.Dig("log").AsString())
}

func TestCRIFull(t *testing.T) {
	root := insaneJSON.Spawn()
	err := DecodeCRI(root, []byte("2016-10-06T00:17:09.669794202Z stdout F full content 2\n"))

	assert.NoError(t, err, "error while decoding cri log")
	assert.Equal(t, "2016-10-06T00:17:09.669794202Z", root.Dig("time").AsString())
	assert.Equal(t, "stdout", root.Dig("stream").AsString())
	assert.Equal(t, "full content 2\n", root.Dig("log").AsString())
}
