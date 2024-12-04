package test

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The file_base contains helpers function for testing file base output plugins
// output/file and output/s3

type Msg []byte

func SendPack(t *testing.T, p *pipeline.Pipeline, msgs []Msg) int64 {
	t.Helper()
	var sent int64 = 0
	for _, m := range msgs {
		_ = p.In(0, "test", Offset(0), m, false, nil)
		// count \n
		sent += int64(len(m)) + 1
	}
	return sent
}

func ClearDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("could not delete dirs and files after tests, error: %s", err.Error())
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
	if info.Size() == 0 {
		assert.NotZero(t, info.Size(), msg)
	}
	return info.Size()
}

func CountLines(t *testing.T, pattern string) int {
	matches, err := filepath.Glob(pattern)
	assert.NoError(t, err)
	lineCount := 0
	for _, match := range matches {
		file, err := os.Open(match)
		require.NoError(t, err)

		fileScanner := bufio.NewScanner(file)
		for fileScanner.Scan() {
			lineCount++
		}

		require.NoError(t, file.Close())
	}
	return lineCount
}

func WaitProcessEvents(t *testing.T, count int, checkInterval, maxTime time.Duration, pattern string) {
	tf := time.Now().Add(maxTime)
	for tf.After(time.Now()) {
		if count <= CountLines(t, pattern) {
			return
		}
		time.Sleep(checkInterval)
	}
}
