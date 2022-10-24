//go:build e2e

package main

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/test"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
General interface for e2e tests
AddConfigSettings - function that preparing а config for your test
Send - function that send message in pipeline
Validate - function that validate result of the work
*/

type e2eTest interface {
	AddConfigSettings(conf *cfg.Config, t *testing.T)
	Send(t *testing.T)
	Validate(t *testing.T)
}

func startForTest(e e2eTest, configPath string, t *testing.T) {
	conf := cfg.NewConfigFromFile(configPath)
	e.AddConfigSettings(conf, t)
	fileD = fd.New(conf, ":9000")
	fileD.Start()
}

func TestE2EStabilityWorkCase(t *testing.T) {
	ff := &fileFile{
		outputNamePattern: "/file-d.log",
		filePattern:       "pod_ns_container-*",
		count:             10,
		lines:             500,
		retTime:           "1s",
	}
	cfgPath := "./../testdata/config/e2e_cfg/file_file.yaml"
	startForTest(ff, cfgPath, t)
	ff.Send(t)
	ff.Validate(t)
}

// Struct for file-file plugin e2e test
type fileFile struct {
	filesDir          string
	filePattern       string
	outputNamePattern string
	count             int
	lines             int
	retTime           string
}

func (f *fileFile) AddConfigSettings(conf *cfg.Config, t *testing.T) {
	f.filesDir = t.TempDir()
	offsetsDir := t.TempDir()
	fmt.Println(f.filesDir)
	input := conf.Pipelines["test"].Raw.Get("input")
	input.Set("watching_dir", f.filesDir)
	input.Set("filename_pattern", f.filePattern)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines["test"].Raw.Get("output")
	output.Set("target_file", f.filesDir+f.outputNamePattern)
	output.Set("retention_interval", f.retTime)
}

func (f *fileFile) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(f.count)
	for i := 0; i < f.count; i++ {
		go func() {
			u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(f.filesDir, "pod_ns_container-"+u1+u2+".log")
			file, err := os.Create(name)
			if err != nil {
				assert.NoError(t, err, "unexpected error")
			}

			file.WriteString(strings.Repeat("{\"first_field\":\"second_field\"}\n", f.lines))
			_ = file.Close()
			wg.Done()
		}()
	}
	wg.Wait()
	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (f *fileFile) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", f.filesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, f.count*f.lines, test.CountLines(t, logFilePattern))
}
