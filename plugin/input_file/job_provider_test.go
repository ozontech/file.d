package input_file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseOffsets(t *testing.T) {
	data := `- stream: firstStream
  default: 100
  another: 200
- stream: secondStream
  stderr: 300
`
	offsets := parseOffsets(data)

	stream, has := offsets["firstStream"]
	assert.True(t, has, "Stream not found")

	offset, has := stream["default"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(100), offset, "Wrong offset")

	offset, has = stream["another"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(200), offset, "Wrong offset")

	stream, has = offsets["secondStream"]
	assert.True(t, has, "Stream not found")

	offset, has = stream["stderr"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(300), offset, "Wrong offset")
}
