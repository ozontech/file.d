package offset

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testOffset struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func (o *testOffset) set(name string, value int) {
	o.Name = name
	o.Value = value
}

func TestYAML(t *testing.T) {
	offset := testOffset{}
	offset.set("some_name", 123)

	buffer := &bytes.Buffer{}
	err := (&yamlValue{&offset}).Save(buffer)
	assert.NoError(t, err)

	fmt.Println(buffer.String())

	loaded := testOffset{}
	err = (&yamlValue{&loaded}).Load(buffer)
	assert.NoError(t, err)

	assert.Equal(t, offset, loaded)
}

func TestSaveLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.yaml")
	offset := testOffset{}
	offset.set("some_name", 123)

	err := SaveYAML(path, &offset)
	assert.NoError(t, err)

	loaded := testOffset{}
	err = LoadYAML(path, &loaded)
	assert.NoError(t, err)

	assert.Equal(t, offset, loaded)
}

func TestAppendFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.yaml")
	for i := 1; i < 5; i++ {
		offset := testOffset{}
		offset.set(fmt.Sprintf("iter_%d", i), i)

		err := SaveYAML(path, &offset)
		assert.NoError(t, err)

		loaded := testOffset{}
		err = LoadYAML(path, &loaded)
		assert.NoError(t, err)

		assert.Equal(t, offset, loaded)
	}
}

// check, that no errors will happen
func TestNoFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.yaml")

	loaded := testOffset{}
	err := LoadYAML(path, &loaded)
	assert.NoError(t, err)
}
