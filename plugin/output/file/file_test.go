package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ozonru/file.d/pipeline"
	"github.com/stretchr/testify/assert"
)

const (
	targetFile = "filetests/log.log"
)

func createFile(t *testing.T, fileName string, data *[]byte) *os.File {
	t.Helper()

	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0666))
	if err != nil {
		t.Fatalf("could not open or create file, error: %s", err.Error())
	}
	if data != nil {
		if _, err := file.Write(*data); err != nil {
			t.Fatalf("could not write cintent into the file, err: %s", err.Error())
		}
	}
	return file
}

func createDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		t.Fatalf("could not create target dir: %s, error: %s", dir, err.Error())
	}
}

func clearDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("coudl not delete dirs and files adter tests, error: %s", err.Error())
	}

}

func getMatches(t *testing.T, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	assert.NoError(t, err)
	return matches
}

func TestShouldSealUpWithContent(t *testing.T) {
	testsCasesFalse := []struct {
		retention time.Duration
	}{
		{time.Minute},
		{10 * time.Minute},
		{1 * time.Hour},
		{1 * time.Hour},
	}

	testsCasesTrue := []struct {
		retention time.Duration
	}{
		{time.Microsecond},
		{5 * time.Microsecond},
		{10 * time.Microsecond},
		{1 * time.Second},
	}
	targetDir, _ := filepath.Split(targetFile)
	createDir(t, targetDir)
	defer clearDir(t, targetDir)
	d := []byte("some data")
	file := createFile(t, targetFile, &d)
	defer file.Close()
	t.Run("do not need seal up, new file", func(t *testing.T) {
		for _, tc := range testsCasesFalse {

			cfg := Config{
				TargetFile:         targetFile,
				RetentionInterval_: tc.retention,
			}

			p := Plugin{
				config:    &cfg,
				targetDir: targetDir,
			}

			result := p.shouldSealUp()
			assert.False(t, result)

		}
	})

	t.Run("need seal up", func(t *testing.T) {
		time.Sleep(2 * time.Second)
		for _, tc := range testsCasesTrue {

			cfg := Config{
				TargetFile:         targetFile,
				RetentionInterval_: tc.retention,
			}

			p := Plugin{
				config:    &cfg,
				targetDir: targetDir,
			}

			result := p.shouldSealUp()
			assert.True(t, result)

		}
	})

}

func TestShouldSealUpNoContent(t *testing.T) {
	testsCasesTimeFalse := []struct {
		retention time.Duration
	}{
		{time.Minute},
		{10 * time.Minute},
		{1 * time.Hour},
		{1 * time.Hour},
	}

	testsCasesTimeTrue := []struct {
		retention time.Duration
	}{
		{time.Microsecond},
		{5 * time.Microsecond},
		{10 * time.Microsecond},
		{1 * time.Second},
	}
	targetDir, _ := filepath.Split(targetFile)
	createDir(t, targetDir)
	defer clearDir(t, targetDir)
	file := createFile(t, targetFile, nil)
	defer file.Close()
	t.Run("do not need seal up, new file, empty file", func(t *testing.T) {
		for _, tc := range testsCasesTimeFalse {

			cfg := Config{
				TargetFile:         targetFile,
				RetentionInterval_: tc.retention,
			}

			p := Plugin{
				config:    &cfg,
				targetDir: targetDir,
			}

			result := p.shouldSealUp()
			assert.False(t, result)

		}
	})
	t.Run("need seal up", func(t *testing.T) {
		time.Sleep(2 * time.Second)
		for _, tc := range testsCasesTimeTrue {

			cfg := Config{
				TargetFile:         targetFile,
				RetentionInterval_: tc.retention,
			}

			p := Plugin{
				config:    &cfg,
				targetDir: targetDir,
			}

			result := p.shouldSealUp()
			assert.False(t, result)

		}
	})

}

func TestGetStartIdx(t *testing.T) {
	testsCases := []struct {
		filesName   []string
		expectedIdx int
	}{
		{[]string{}, 0},
		{[]string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06.log"}, 3},
		{[]string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06.log", "filetests/log_3_07.log"}, 4},
		{[]string{"filetests/log_-2_06.log", "filetests/log_-3_06.log", "filetests/log_-1_06.log"}, 0},
		{[]string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06"}, 2},
		{[]string{"filetests/log_-2_06.log", "filetests/log_-3_06.log", "filetests/log_-4_06.log"}, 0},
		{[]string{"filetests/another_0_06.log", "filetests/another_1_06.log", "filetests/another_2_06.log"}, 0},
	}
	cfg := Config{
		TargetFile: targetFile,
		Layout:     "01",
	}

	dir, file := filepath.Split(cfg.TargetFile)
	extension := filepath.Ext(file)

	p := Plugin{
		config:        &cfg,
		targetDir:     dir,
		fileExtension: extension,
		fileName:      file[0 : len(file)-len(extension)],
	}

	for _, tc := range testsCases {

		//create files
		files := make([]*os.File, len(tc.filesName))
		createDir(t, p.targetDir)
		defer clearDir(t, p.targetDir)

		for _, f := range tc.filesName {
			files = append(files, createFile(t, f, nil))
		}
		//	make check
		idx := p.getStartIdx()
		assert.EqualValues(t, tc.expectedIdx, idx)

		//	close files
		for _, f := range files {
			f.Close()
		}
		clearDir(t, p.targetDir)

	}
}

func TestSealUpHasContent(t *testing.T) {
	cfg := Config{
		TargetFile:         targetFile,
		RetentionInterval_: time.Second,
		Layout:             "01",
		FileMode_:          0666,
	}

	dir, file := filepath.Split(cfg.TargetFile)

	//cfg.TargetDir = dir
	extension := filepath.Ext(file)

	createDir(t, dir)

	defer clearDir(t, dir)

	d := []byte("some data")
	f := createFile(t, cfg.TargetFile, &d)
	defer f.Close()
	p := Plugin{
		config:        &cfg,
		mu:            &sync.RWMutex{},
		file:          f,
		targetDir:     dir,
		fileExtension: extension,
		fileName:      file[0 : len(file)-len(extension)],
	}

	infoInitial, _ := f.Stat()
	assert.NotZero(t, infoInitial.Size())

	//call func
	p.sealUp()

	//check work result
	pattern := fmt.Sprintf("%s/%s*%s", p.targetDir, p.fileName, p.fileExtension)
	matches := getMatches(t, pattern)
	assert.Equal(t, 2, len(matches))

	//check new file was created and it is empty
	info, err := p.file.Stat()
	assert.EqualValues(t, 0, info.Size())
	assert.NoError(t, err)

	//check old file was renamed. And renamed file is not empty and contains data
	for _, v := range matches {
		if v != cfg.TargetFile {
			info, err := os.Stat(v)
			assert.NoError(t, err)
			assert.EqualValues(t, len(d), info.Size())
		}
	}
}

func TestSealUpNoContent(t *testing.T) {
	cfg := Config{
		TargetFile:         targetFile,
		RetentionInterval_: time.Second,
		Layout:             "01",
		FileMode_:          0666,
	}

	dir, file := filepath.Split(cfg.TargetFile)
	extension := filepath.Ext(file)

	createDir(t, dir)

	defer clearDir(t, dir)

	f := createFile(t, cfg.TargetFile, nil)
	defer f.Close()
	p := Plugin{
		config:        &cfg,
		mu:            &sync.RWMutex{},
		file:          f,
		targetDir:     dir,
		fileExtension: extension,
		fileName:      file[0 : len(file)-len(extension)],
	}

	infoInitial, _ := f.Stat()
	assert.Zero(t, infoInitial.Size())

	//call func
	p.sealUp()

	//check work result
	pattern := fmt.Sprintf("%s/%s*%s", p.targetDir, p.fileName, p.fileExtension)
	assert.Equal(t, 1, len(getMatches(t, pattern)))

	//check new file was created and it is empty
	info, err := p.file.Stat()
	assert.Zero(t, info.Size())
	assert.NoError(t, err)
}

func TestStart(t *testing.T) {
	clearDir(t, "filetests")
	cfg := Config{
		TargetFile:         targetFile,
		RetentionInterval_: 2 * time.Second,
		Layout:             "01",
		BatchSize_:         4,
		BatchFlushTimeout_: time.Second,
		FileMode_:          0666,
	}
	pluginParams := pipeline.OutputPluginParams{
		PluginDefaultParams: &pipeline.PluginDefaultParams{
			PipelineName: "name",
			PipelineSettings: &pipeline.Settings{
				Capacity:   128,
				AvgLogSize: 128,
			},
		},
		Controller: nil,
	}
	p := Plugin{}

	p.Start(&cfg, &pluginParams)

	defer p.Stop()
	defer clearDir(t, p.targetDir)

	//check no saves without events

	pattern := fmt.Sprintf("%s/%s*%s", p.targetDir, p.fileName, p.fileExtension)

	assert.Equal(t, 1, len(getMatches(t, pattern)))
	assert.NotEqual(t, "", p.targetDir)
	assert.NotEqual(t, "", p.fileExtension)
	assert.NotEqual(t, "", p.fileName)

	assert.Equal(t, "filetests/", p.targetDir)
	assert.Equal(t, ".log", p.fileExtension)
	assert.Equal(t, "log", p.fileName)

	assert.False(t, p.nextSealUpTime.IsZero())

}
