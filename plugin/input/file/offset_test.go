package file

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

func TestParseOffsets(t *testing.T) {
	data := `- file: /some/informational/name
  inode: 1
  source_id: 1234
  streams:
    default: 100
    another: 200
- file: /another/informational/name
  inode: 2
  source_id: 4321
  streams:
    stderr: 300
`
	offsetDB := newOffsetDB("", "")
	offsets := offsetDB.parse(data)

	item, has := offsets[pipeline.SourceID(1234)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/some/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(1234), item.sourceID)

	offset, has := item.streams["default"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(100), offset, "wrong offset")

	offset, has = item.streams["another"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(200), offset, "wrong offset")

	item, has = offsets[pipeline.SourceID(4321)]
	assert.True(t, has, "item isn't found")

	assert.Equal(t, "/another/informational/name", item.filename)
	assert.Equal(t, pipeline.SourceID(4321), item.sourceID)

	offset, has = item.streams["stderr"]
	assert.True(t, has, "stream isn't found")
	assert.Equal(t, int64(300), offset, "wrong offset")
}
