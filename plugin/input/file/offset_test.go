package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseOffsets(t *testing.T) {
	data := `- file: /some/informational/name
  inode: 1
  fingerprint: 1234
  streams:
    default: 100
    another: 200
- file: /another/informational/name
  inode: 2
  fingerprint: 4321
  streams:
    stderr: 300
`
	offsetDB := newOffsetDB("", "")
	offsets := offsetDB.parse(data)

	inode, has := offsets[1]
	assert.True(t, has, "Stream not found")

	assert.Equal(t, "/some/informational/name", inode.filename)
	assert.Equal(t, fingerprint(1234), inode.fingerprint)

	offset, has := inode.streams["default"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(100), offset, "Wrong offset")

	offset, has = inode.streams["another"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(200), offset, "Wrong offset")

	inode, has = offsets[2]
	assert.True(t, has, "Stream not found")

	assert.Equal(t, "/another/informational/name", inode.filename)
	assert.Equal(t, fingerprint(4321), inode.fingerprint)

	offset, has = inode.streams["stderr"]
	assert.True(t, has, "Sub stream not found")
	assert.Equal(t, int64(300), offset, "Wrong offset")
}
