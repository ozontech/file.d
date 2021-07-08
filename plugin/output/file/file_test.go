package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ozonru/file.d/cfg"
	"github.com/stretchr/testify/assert"
)

const (
	targetFile = "filetests/log.log"
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
		cfg := Config{
			TargetFile: tc.targetFile,
			Layout:     tc.layout,
		}

		dir, file := filepath.Split(cfg.TargetFile)
		extension := filepath.Ext(file)
		clearDir(t, dir)
		p := Plugin{
			config:        &cfg,
			targetDir:     dir,
			fileExtension: extension,
			fileName:      file[0 : len(file)-len(extension)],
		}

		//create files
		files := make([]*os.File, len(tc.filesName))
		createDir(t, p.targetDir)
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
	fileSealUpInterval = 200 * time.Millisecond
	cfg := Config{
		TargetFile:         targetFile,
		RetentionInterval_: 200 * time.Millisecond,
		Layout:             "01",
		FileMode_:          0666,
	}

	dir, file := filepath.Split(cfg.TargetFile)

	//cfg.TargetDir = dir
	extension := filepath.Ext(file)
	clearDir(t, dir)
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
	fileSealUpInterval = 200 * time.Millisecond
	cfg := Config{
		TargetFile:         targetFile,
		RetentionInterval_: 200 * time.Millisecond,
		Layout:             "01",
		FileMode_:          0666,
	}

	dir, file := filepath.Split(cfg.TargetFile)
	extension := filepath.Ext(file)

	clearDir(t, dir)
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

type msg []byte

func TestStart(t *testing.T) {
	tests := struct {
		firstPack  []msg
		secondPack []msg
		thirdPack  []msg
	}{
		firstPack: []msg{
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		secondPack: []msg{
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
		thirdPack: []msg{
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
			msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"3ea4a6589d06bb3f","span_id":"deddd718684b10a","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_cancelled"}`),
		},
	}
	clearDir(t, dir)
	defer clearDir(t, dir)
	config := &Config{
		TargetFile:        targetFile,
		RetentionInterval: "300ms",
		Layout:            "01",
		BatchFlushTimeout: "100ms",

		FileMode_: 0666,
	}
	fileSealUpInterval = 200 * time.Millisecond

	pattern := fmt.Sprintf("%s/*%s", dir, extension)

	writeFileSleep := 100*time.Millisecond + 100*time.Millisecond
	sealUpFileSleep := 2*fileSealUpInterval + 500*time.Millisecond

	err := cfg.Parse(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	assert.NoError(t, err)
	totalSent := int64(0)

	p := newPipeline(t, config)
	assert.NotNil(t, p, "could not create new pipline")
	p.Start()
	time.Sleep(300 * time.Microsecond)

	//check log file created and empty
	checkZero(t, config.TargetFile, "log file is not created or is not empty")

	//send events
	packSize := sendPack(t, p, tests.firstPack)
	totalSent += packSize
	time.Sleep(writeFileSleep)

	// check that plugin wrote into the file
	assert.Equal(t, packSize, checkNotZero(t, config.TargetFile, "check log file has data"), "plugin did not write into the file")

	time.Sleep(sealUpFileSleep)
	//check sealing up
	//check log file is empty
	checkZero(t, config.TargetFile, "log fil is not empty after sealing up")

	//check that sealed up file is created and not empty
	matches := getMatches(t, pattern)

	assert.GreaterOrEqual(t, len(matches), 2, "there is no new file after sealing up")
	checkDirFiles(t, matches, totalSent, "written data and saved data are not equal")

	//send next pack. And stop pipeline before next seal up time
	totalSent += sendPack(t, p, tests.secondPack)
	time.Sleep(writeFileSleep)
	// check that plugin wrote into the file
	p.Stop()
	// check that plugin  did not seal up
	checkNotZero(t, config.TargetFile, "plugin sealed up")

	matches = getMatches(t, pattern)
	checkDirFiles(t, matches, totalSent, "after sealing up interruption written data and saved data are not equal")

	time.Sleep(sealUpFileSleep)
	// Start new pipeline like pod restart
	//start pipeline again
	p2 := newPipeline(t, config)
	p2.Start()
	//waite ticker 1st tick
	time.Sleep(fileSealUpInterval + 50*time.Millisecond)
	// check old file log file is sealed up
	matches = getMatches(t, pattern)
	assert.GreaterOrEqual(t, len(matches), 3, "old log file is not sealed up")
	checkZero(t, targetFile, "log file is not empty after sealing up")

	//send third pack
	totalSent += sendPack(t, p2, tests.thirdPack)
	time.Sleep(writeFileSleep)
	checkNotZero(t, config.TargetFile, "third pack is not written")
	matches = getMatches(t, pattern)
	checkDirFiles(t, matches, totalSent, "")

	// check seal up for third
	time.Sleep(sealUpFileSleep)
	checkZero(t, config.TargetFile, "log file with third pack is not sealed up")
	matches = getMatches(t, pattern)
	assert.GreaterOrEqual(t, len(matches), 4, "there is no new files after sealing up third pack")

	checkDirFiles(t, matches, totalSent, "lost data for third pack")
	p2.Stop()
}
