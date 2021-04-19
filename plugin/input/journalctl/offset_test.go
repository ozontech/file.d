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
	for i := 1; i < 6; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
	}
	offset.current = offset.offset + 1

	buffer := &bytes.Buffer{}
	err := offset.Write(buffer)
	assert.NoError(t, err)

	fmt.Println(buffer.String())

	loaded := newOffsetInfo("")
	err = loaded.Read(buffer)
	assert.NoError(t, err)

	compare(t, offset, loaded)
}

func TestSaveLoadFile(t *testing.T) {
	path := getTmpPath(t, "offset.yaml")
	offset := newOffsetInfo(path)
	for i := 1; i < 6; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
	}
	offset.current = offset.offset + 1

	err := offset.save()
	assert.NoError(t, err)

	loaded := newOffsetInfo(path)
	err = loaded.load()
	assert.NoError(t, err)

	compare(t, offset, loaded)
}

func TestAppendFile(t *testing.T) {
	path := getTmpPath(t, "offset.yaml")
	offset := newOffsetInfo(path)
	for i := 1; i < 6; i++ {
		offset.set(fmt.Sprintf("cursor_%d", i))
		offset.current = offset.offset + 1

		err := offset.save()
		assert.NoError(t, err)
	
		loaded := newOffsetInfo(path)
		err = loaded.load()
		assert.NoError(t, err)
	
		compare(t, offset, loaded)
	}
}

// check, that no errors will happen
func TestEmptyFile(t *testing.T) {
	offset := newOffsetInfo(getTmpPath(t, "offset.yaml"))

	err := offset.load()
	assert.NoError(t, err)

	compare(t, newOffsetInfo(""), offset)
}
