package file

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	targetFile          = "filetests/log.log"
	targetFileThreshold = "filetests/%d%slog.log"
)

var (
	dir, file = filepath.Split(targetFile)
	extension = filepath.Ext(file)
)

func TestGetStartIdx(t *testing.T) {
	testsCases := []struct {
		targetFile  string
		layout      string
		filesName   []string
		expectedIdx int
	}{
		{targetFile, "01", []string{}, 0},
		{targetFile, "01", []string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06.log"}, 3},
		{targetFile, "01", []string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06.log", "filetests/log_3_07.log"}, 4},
		{targetFile, "01", []string{"filetests/log_-2_06.log", "filetests/log_-3_06.log", "filetests/log_-1_06.log"}, 0},
		{targetFile, "01", []string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06"}, 2},
		{targetFile, "01", []string{"filetests/log_-2_06.log", "filetests/log_-3_06.log", "filetests/log_-4_06.log"}, 0},
		{targetFile, "01", []string{"filetests/another_0_06.log", "filetests/another_1_06.log", "filetests/another_2_06.log"}, 0},

		{"filetests/some-file", "01-02", []string{"filetests/some-file_0_06-02.log"}, 0},
		{"filetests/some-file", "01-02", []string{"filetests/some-file_0_06-02"}, 1},
		{"tmp/f.log", "", []string{"tmp/f_0_.log", "tmp/f_8_.log"}, 9},
		{"tmp/f.log.log", "", []string{"tmp/f.log_0_.log"}, 1},
		{"tmp/f_0_3.log", "01", []string{"tmp/f_0_3_0_05.log"}, 1},
		{"tmp/f_0_3.log", "01", []string{"tmp/f_3_0_05.log"}, 0},
	}

	for _, tc := range testsCases {
		config := Config{
			TargetFile: tc.targetFile,
			Layout:     tc.layout,
		}

		dir, file := filepath.Split(config.TargetFile)
		extension := filepath.Ext(file)
		test.ClearDir(t, dir)
		p := Plugin{
			config:        &config,
			targetDir:     dir,
			fileExtension: extension,
			fileName:      file[0 : len(file)-len(extension)],
		}

		// create files.
		files := make([]*os.File, len(tc.filesName))
		createDir(t, p.targetDir)
		for _, f := range tc.filesName {
			files = append(files, createFile(t, f, nil))
		}
		// make check
		idx := p.getStartIdx()
		assert.EqualValues(t, tc.expectedIdx, idx)

		// close files.
		for _, f := range files {
			f.Close()
		}
		test.ClearDir(t, p.targetDir)
	}
}

func TestSealUpHasContent(t *testing.T) {
	config := Config{
		TargetFile:         targetFile,
		RetentionInterval_: 200 * time.Millisecond,
		Layout:             "01",
		FileMode_:          0o666,
	}

	dir, file := filepath.Split(config.TargetFile)

	extension := filepath.Ext(file)
	test.ClearDir(t, dir)
	createDir(t, dir)
	defer test.ClearDir(t, dir)

	d := []byte("some data")
	testFileName := fmt.Sprintf(targetFileThreshold, time.Now().Unix(), fileNameSeparator)
	f := createFile(t, testFileName, &d)
	defer f.Close()
	infoInitial, _ := f.Stat()
	assert.NotZero(t, infoInitial.Size())
	p := Plugin{
		config:        &config,
		mu:            &sync.RWMutex{},
		file:          f,
		targetDir:     dir,
		fileExtension: extension,
		fileName:      file[0 : len(file)-len(extension)],
		tsFileName:    path.Base(testFileName),
	}

	// call func
	p.sealUp()

	// check work result
	pattern := fmt.Sprintf("%s/*%s", p.targetDir, p.fileExtension)
	matches := test.GetMatches(t, pattern)
	assert.Equal(t, 2, len(matches))

	// check new file was created and it is empty
	info, err := p.file.Stat()
	assert.EqualValues(t, 0, info.Size())
	assert.NoError(t, err)

	// check old file was renamed. And renamed file is not empty and contains data
	for _, v := range matches {
		if v != fmt.Sprintf("%s%s", dir, p.tsFileName) {
			info, err := os.Stat(v)
			assert.NoError(t, err)
			assert.EqualValues(t, len(d), info.Size())
		}
	}
}

func TestSealUpNoContent(t *testing.T) {
	config := Config{
		TargetFile:         targetFile,
		RetentionInterval_: 200 * time.Millisecond,
		Layout:             "01",
		FileMode_:          0o666,
	}

	dir, file := filepath.Split(config.TargetFile)
	extension := filepath.Ext(file)

	test.ClearDir(t, dir)
	createDir(t, dir)
	defer test.ClearDir(t, dir)
	testFileName := fmt.Sprintf(targetFileThreshold, time.Now().Unix(), fileNameSeparator)
	f := createFile(t, testFileName, nil)
	defer f.Close()
	p := Plugin{
		config:        &config,
		mu:            &sync.RWMutex{},
		file:          f,
		targetDir:     dir,
		fileExtension: extension,
		fileName:      file[0 : len(file)-len(extension)],
		tsFileName:    path.Base(testFileName),
	}

	infoInitial, _ := f.Stat()
	assert.Zero(t, infoInitial.Size())

	// call func
	p.sealUp()

	// check work result
	pattern := fmt.Sprintf("%s/*%s", p.targetDir, p.fileExtension)
	assert.Equal(t, 1, len(test.GetMatches(t, pattern)))

	// check new file was created and it is empty
	info, err := p.file.Stat()
	assert.Zero(t, info.Size())
	assert.NoError(t, err)
}

func TestStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}
	tests := struct {
		firstPack  []test.Msg
		secondPack []test.Msg
		thirdPack  []test.Msg
	}{
		firstPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	config := &Config{
		TargetFile:        targetFile,
		RetentionInterval: "2s",
		Layout:            "01",
		BatchFlushTimeout: "100ms",

		FileMode_: 0o666,
	}

	writeFileSleep := 4 * 100 * time.Millisecond
	sealUpFileSleep := 2 * time.Second
	generalPattern := fmt.Sprintf("%s/*%s", dir, extension)
	logFilePattern := fmt.Sprintf("%s/*%s", path.Dir(targetFile), path.Base(targetFile))
	currentLogFileSubstr := fmt.Sprintf("_%s", path.Base(targetFile))
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	totalSent := int64(0)
	p := newPipeline(t, config)
	assert.NotNil(t, p, "could not create new pipeline")

	p.Start()

	// check log file created and empty
	matches := test.GetMatches(t, logFilePattern)
	require.Equal(t, 1, len(matches))

	tsFileName := matches[0]
	test.CheckZero(t, tsFileName, "log file is not created or is not empty")
	logger.Errorf("tsFileName=%s", tsFileName)

	// send events
	logger.Errorf("send pack, t=%s", time.Now().Unix())
	packSize := test.SendPack(t, p, tests.firstPack)
	totalSent += packSize
	time.Sleep(writeFileSleep)
	logger.Errorf("after sleep")

	// check that plugin wrote into the file
	require.Equal(t, packSize, test.CheckNotZero(t, tsFileName, "check log file has data"), "plugin did not write into the file")
	time.Sleep(sealUpFileSleep)
	// check sealing up
	// check log file is empty
	matches = test.GetMatches(t, logFilePattern)
	assert.Equal(t, 1, len(matches))
	tsFileName = matches[0]
	test.CheckZero(t, tsFileName, "log fil is not empty after sealing up")

	// check that sealed up file is created and not empty
	matches = test.GetMatches(t, generalPattern)
	assert.GreaterOrEqual(t, len(matches), 2, "there is no new file after sealing up")
	checkDirFiles(t, matches, totalSent, "written data and saved data are not equal")

	// send next pack. And stop pipeline before next seal up time
	totalSent += test.SendPack(t, p, tests.secondPack)
	time.Sleep(writeFileSleep)
	// check that plugin wrote into the file
	p.Stop()
	// check that plugin  did not seal up
	matches = test.GetMatches(t, generalPattern)
	checkDirFiles(t, matches, totalSent, "after sealing up interruption written data and saved data are not equal")
	for _, m := range matches {
		if strings.Contains(m, currentLogFileSubstr) {
			test.CheckNotZero(t, m, "plugin sealed up")
			break
		}
	}

	time.Sleep(sealUpFileSleep)
	// Start new pipeline like pod restart
	// start pipeline again
	p2 := newPipeline(t, config)
	p2.Start()
	// waite ticker 1st tick
	time.Sleep(250 * time.Millisecond)
	// check old file log file is sealed up
	matches = test.GetMatches(t, generalPattern)
	assert.GreaterOrEqual(t, len(matches), 3, "old log file is not sealed up")

	for _, m := range matches {
		if strings.Contains(m, currentLogFileSubstr) {
			test.CheckZero(t, m, "log file is not empty after sealing up")
			break
		}
	}

	// send third pack
	totalSent += test.SendPack(t, p2, tests.thirdPack)
	time.Sleep(writeFileSleep)

	matches = test.GetMatches(t, generalPattern)
	checkDirFiles(t, matches, totalSent, "")
	for _, m := range matches {
		if strings.Contains(m, currentLogFileSubstr) {
			test.CheckNotZero(t, m, "third pack is not written")
		}
	}

	// check seal up for third
	time.Sleep(sealUpFileSleep)
	matches = test.GetMatches(t, generalPattern)
	assert.GreaterOrEqual(t, len(matches), 4, "there is no new plugins after sealing up third pack")
	checkDirFiles(t, matches, totalSent, "lost data for third pack")
	for _, m := range matches {
		if strings.Contains(m, currentLogFileSubstr) {
			test.CheckZero(t, m, "log file with third pack is not sealed up")
			break
		}
	}
	p2.Stop()
}
