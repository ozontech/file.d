package file

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	targetFile = "filetests/log.log"
)

var (
	dir, file = filepath.Split(targetFile)
	extension = filepath.Ext(file)
)

func TestNextSealUpIdx(t *testing.T) {
	tests := []struct {
		name       string
		targetFile string
		files      []string
		want       int
	}{
		{
			name:       "no_files",
			targetFile: targetFile,
			files:      []string{},
			want:       0,
		},
		{
			name:       "sealed_up_files",
			targetFile: targetFile,
			files:      []string{"filetests/log_0_06.log", "filetests/log_1_06.log", "filetests/log_2_06.log"},
			want:       3,
		},
		{
			name:       "negative_indexes",
			targetFile: targetFile,
			files:      []string{"filetests/log_-2_06.log", "filetests/log_-3_06.log", "filetests/log_-1_06.log"},
			want:       0,
		},
		{
			name:       "another_extension",
			targetFile: "log.log",
			files:      []string{"log_0_06.log", "log_5_06"},
			want:       1,
		},
		{
			name:       "another_name",
			targetFile: "log.log",
			files:      []string{"another_0_06.log"},
			want:       0,
		},
		{
			name:       "compressed",
			targetFile: "log.log",
			files:      []string{"log_0_06.log.gz", "log_1_06.log.zst", "log_2_06.log.lz4"},
			want:       3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir() + "/"
			for _, f := range tt.files {
				createDir(t, filepath.Dir(dir+f))
				createFile(t, dir+f, nil).Close()
			}

			dir, file := filepath.Split(dir + tt.targetFile)
			p := newTestPlugin(t, &Config{TimeLayout: "01"})

			got := p.nextSealUpIdx(&activeFile{dir: dir, name: strings.TrimSuffix(file, extension), ext: extension})
			require.Equal(t, tt.want, got)
		})
	}
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
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"1","span_id":"1","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"2","span_id":"2","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_1","trace_id":"3","span_id":"3","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		secondPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"4","span_id":"4","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"5","span_id":"5","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_12","trace_id":"6","span_id":"6","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
		thirdPack: []test.Msg{
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"7","span_id":"7","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"8","span_id":"8","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
			test.Msg(`{"level":"error","ts":"2019-08-21T11:43:25.865Z","message":"get_items_error_123","trace_id":"9","span_id":"9","get_items_error":"product: error while consuming CoverImage: context canceled","get_items_error_option":"CoverImage","get_items_error_cause":"context canceled","get_items_error_cause_type":"context_canceled"}`),
		},
	}
	test.ClearDir(t, dir)
	defer test.ClearDir(t, dir)
	config := &Config{
		TargetFile:        targetFile,
		RetentionInterval: "2s",
		TimeLayout:        "2006-01-02",
		BatchFlushTimeout: "100ms",
		FileMode_:         0o666,
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
