package s3

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestGetName(t *testing.T) {
	c := newZipCompressor(logger.Instance)
	testsCases := map[string]string{
		"some":                "some.zip",
		"awesome.log":         "awesome.log.zip",
		"dir/subdir/file.log": "dir/subdir/file.log.zip",
	}

	for k, v := range testsCases {
		zipName := c.getName(k)
		assert.Equal(t, v, zipName)
	}
}

type simpleWriter struct{}

func (w simpleWriter) Write(p []byte) (n int, err error) {
	if string(p) == logStr {
		return 0, nil
	}
	return -1, nil
}

var logStr = "I am a log line in a file\n"

func TestCompress(t *testing.T) {
	dir := "tests"
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)

	err := os.MkdirAll(dir, os.ModePerm)
	assert.NoError(t, err)

	c := newZipCompressor(logger.Instance)
	testsCases := []string{
		"tests/file1.log",
		"tests/file2.log",
		"tests/some.log",
	}

	// create files and write data
	for _, v := range testsCases {
		file, err := os.OpenFile(v, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o666))
		assert.NoError(t, err)
		_, err = file.WriteString(logStr)
		assert.NoError(t, err)
		err = file.Close()
		assert.NoError(t, err)
	}

	// then compress them
	for _, v := range testsCases {
		c.compress(c.getName(v), v)
	}

	// check zips and content in
	matches := test.GetMatches(t, fmt.Sprintf("%s/*.log.zip", dir))
	assert.Equal(t, len(testsCases), len(matches))
	for _, m := range matches {
		test.CheckNotZero(t, m, "empty archive")
		r, err := zip.OpenReader(m)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(r.File))
		closer, err := r.File[0].Open()
		assert.NoError(t, err)
		sw := simpleWriter{}
		written, err := io.CopyN(sw, closer, int64(len(logStr)))
		assert.Equal(t, int64(0), written)
		err = closer.Close()
		assert.NoError(t, err)
		err = r.Close()
		assert.NoError(t, err)

	}
}
