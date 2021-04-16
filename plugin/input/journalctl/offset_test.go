package journalctl

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func compare(t *testing.T, exp *offsetInfo, act *offsetInfo) {
	assert.Equal(t, exp.current, act.current)
	assert.Equal(t, exp.offset, act.offset)
	assert.Equal(t, exp.cursor, act.cursor)
}

func TestSaveLoad(t *testing.T) {
	offset := newOffsetInfo("")
	for i := 0; i < 5; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
	}
	offset.current = offset.offset + 1

	buffer := &bytes.Buffer{}
	err := offset.save(buffer)
	assert.NoError(t, err)

	fmt.Println(buffer.String())

	loaded := newOffsetInfo("")
	err = loaded.load(buffer)
	assert.NoError(t, err)

	compare(t, offset, loaded)
}

func TestSaveLoadFile(t *testing.T) {
	offset := newOffsetInfo(getTmpPath(t, "offset.yaml"))
	for i := 0; i < 5; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
	}
	offset.current = offset.offset + 1

	err := offset.saveFile()
	assert.NoError(t, err)

	loaded := newOffsetInfo(offset.path)
	err = loaded.loadFile()
	assert.NoError(t, err)

	compare(t, offset, loaded)
}

func TestAppendFile(t *testing.T) {
	offset := newOffsetInfo(getTmpPath(t, "offset.yaml"))
	for i := 0; i < 5; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
		offset.current = offset.offset + 1
		err := offset.saveFile()
		assert.NoError(t, err)

		loaded := newOffsetInfo(offset.path)
		err = loaded.loadFile()
		assert.NoError(t, err)

		compare(t, offset, loaded)
	}
}
