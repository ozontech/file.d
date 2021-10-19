package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ozonru/file.d/pipeline"
	"github.com/stretchr/testify/assert"
)

// The file contains helpers function for testing file base output plugins
// output/file and output/s3

type Msg []byte

func SendPack(t *testing.T, p *pipeline.Pipeline, msgs []Msg) int64 {
	t.Helper()
	var sent int64 = 0
	for _, m := range msgs {
		p.In(0, "test", 0, m, false)
		// count \n
		sent += int64(len(m)) + 1
	}
	return sent
}

func ClearDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("coudl not delete dirs and files adter tests, error: %s", err.Error())
	}
}

func GetMatches(t *testing.T, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	assert.NoError(t, err)
	return matches
}

func CheckZero(t *testing.T, target string, msg string) int64 {
	t.Helper()
	info, err := os.Stat(target)
	assert.NoErrorf(t, err, "there is no target: %s", target, msg)
	assert.NotNil(t, info, msg)
	assert.Zero(t, info.Size(), msg)
	return info.Size()
}

func CheckNotZero(t *testing.T, target string, msg string) int64 {
	t.Helper()
	info, err := os.Stat(target)
	assert.NoError(t, err, msg)
	assert.NotNil(t, info, msg)
	assert.NotZero(t, info.Size(), msg)
	return info.Size()
}
