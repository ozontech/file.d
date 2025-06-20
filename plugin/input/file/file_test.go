package file

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/prometheus/client_golang/prometheus"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

var (
	filesDir   = ""
	offsetsDir = ""
)

const (
	offsetsFile = "offsets.yaml"
	newLine     = 1
	perm        = 0o770

	strPrefix = `"_`
)

func TestMain(m *testing.M) {
	setupDirs()
	exitVal := m.Run()
	cleanUp()
	os.Exit(exitVal)
}

func setupDirs() {
	f, err := os.MkdirTemp("", "input_file")
	if err != nil {
		panic(err.Error())
	}
	filesDir = f

	f, err = os.MkdirTemp("", "input_file_offsets")
	if err != nil {
		panic(err.Error())
	}
	offsetsDir = f
}

func cleanUp() {
	err := os.RemoveAll(filesDir)
	if err != nil {
		panic(err.Error())
	}

	err = os.Mkdir(filesDir, perm)
	if err != nil {
		panic(err.Error())
	}
}

func pluginConfig(opts ...string) *Config {
	op := ""
	if test.Opts(opts).Has("tail") {
		op = "tail"
	}
	if test.Opts(opts).Has("reset") {
		op = "reset"
	}

	config := &Config{
		WatchingDir:         filesDir,
		OffsetsFile:         filepath.Join(offsetsDir, offsetsFile),
		PersistenceMode:     "async",
		OffsetsOp:           op,
		MaintenanceInterval: "5s",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": runtime.GOMAXPROCS(0)})

	return config
}

func renameFile(oldFile string, newFile string) {
	err := os.Rename(oldFile, newFile)
	if err != nil {
		panic(err.Error())
	}
}

func closeFile(f *os.File) {
	if err := f.Close(); err != nil {
		panic(err.Error())
	}
}

func truncateFile(file string) {
	f, err := os.OpenFile(file, os.O_WRONLY, perm)
	if err != nil {
		panic(err.Error())
	}
	defer closeFile(f)

	err = f.Truncate(0)
	if err != nil {
		panic(err.Error())
	}
}

func rotateFile(file string) string {
	newFile := file + ".new"
	renameFile(file, newFile)
	createFile(file)

	return newFile
}

// any string in opts may contain: tail, dirty
func run(testCase *test.Case, eventCount int, opts ...string) {
	if !test.Opts(opts).Has("dirty") {
		cleanUp()
		setupDirs()
	}

	test.RunCase(testCase, getInputInfo(opts...), eventCount, opts...)
}

func getInputInfo(opts ...string) *pipeline.InputPluginInfo {
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "",
			Factory: nil,
			Config:  pluginConfig(opts...),
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "",
		},
	}
}

func createFile(file string) {
	fd, err := os.Create(file)
	if err != nil {
		panic(err.Error())
	}
	err = fd.Close()
	if err != nil {
		panic(err.Error())
	}
}

func createTempFile() string {
	u := uuid.NewV4().String()
	file, err := os.Create(path.Join(filesDir, u))
	if err != nil {
		panic(err.Error())
	}

	return file.Name()
}

func createOffsetFile() string {
	file, err := os.Create(path.Join(offsetsDir, offsetsFile))
	if err != nil {
		panic(err.Error())
	}

	return file.Name()
}

func addDataFile(file *os.File, data []byte) {
	if _, err := file.Write(data); err != nil {
		panic(err.Error())
	}
}

func addBytes(file string, data []byte, isLine bool, doSync bool) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, perm)
	if err != nil {
		panic(err.Error())
	}
	defer closeFile(f)

	if _, err = f.Write(data); err != nil {
		panic(err.Error())
	}

	if isLine {
		if _, err = f.Write([]byte{'\n'}); err != nil {
			panic(err.Error())
		}
	}

	if doSync {
		err = f.Sync()
		if err != nil {
			panic(err.Error())
		}
	}
}

func addString(file string, str string, isLine bool, doSync bool) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, perm)
	if err != nil {
		panic(err.Error())
	}
	defer closeFile(f)

	if _, err = f.WriteString(str); err != nil {
		panic(err.Error())
	}

	if isLine {
		if _, err = f.Write([]byte{'\n'}); err != nil {
			panic(err.Error())
		}
	}

	if doSync {
		err = f.Sync()
		if err != nil {
			panic(err.Error())
		}
	}
}

func addLines(file string, from int, to int) int {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, perm)
	if err != nil {
		panic(err.Error())
	}

	defer closeFile(f)

	size := 0
	for i := from; i < to; i++ {
		if _, err = f.WriteString(strPrefix); err != nil {
			panic(err.Error())
		}
		size += len(strPrefix)

		str := fmt.Sprintf(`%d"`+"\n", i)
		if _, err = f.WriteString(str); err != nil {
			panic(err.Error())
		}
		size += len(str)
	}

	err = f.Sync()
	if err != nil {
		panic(err.Error())
	}

	return size
}

func getContent(file string) string {
	content, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return string(content)
}

func getContentBytes(file string) []byte {
	content, err := os.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return content
}

func genOffsetsContent(file string, offset int) string {
	return fmt.Sprintf(`- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: %d
`, file, getInodeByFile(file), getSourceID(file), offset)
}

func genOffsetsContentMultiple(files []string, offset int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: %d
`, file, getInodeByFile(file), getSourceID(file), offset)...)
	}

	return string(result)
}

func genOffsetsContentMultipleStreams(files []string, offsetStdErr int, offsetStdOut int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %s
  inode: %d
  source_id: %d
  streams:
    stderr: %d
    stdout: %d
`, file, getInodeByFile(file), getSourceID(file), offsetStdErr, offsetStdOut)...)
	}

	return string(result)
}

func getSourceID(file string) pipeline.SourceID {
	info, _ := os.Stat(file)
	return sourceIDByStat(info, "")
}

func getInodeByFile(file string) uint64 {
	stat, err := os.Stat(file)
	if err != nil {
		panic(err)
	}
	sysStat := stat.Sys().(*syscall.Stat_t)
	inode := sysStat.Ino
	return inode
}

func assertOffsetsAreEqual(t *testing.T, expectedContent string, actualContent string) {
	ctl := metric.NewCtl("test", prometheus.NewRegistry())
	metrics := newOffsetDbMetricCollection(
		ctl.RegisterCounter("worker1", "help_test"),
	)
	offsetDB := newOffsetDB("", "", metrics)
	offExpected, err := offsetDB.parse(expectedContent)
	require.NoError(t, err)
	offActual, err := offsetDB.parse(actualContent)
	require.NoError(t, err)
	for sourceID, inodeExp := range offExpected {
		_, has := offActual[sourceID]
		assert.True(t, has, "offsets aren't equal, source id=%d", sourceID)
		for stream, expected := range inodeExp.streams {
			actual, has := offActual[sourceID].streams[stream]
			assert.True(t, has, "offsets aren't equal, no stream %q", stream)
			assert.Equal(t, expected, actual, "offsets aren't equal")
		}
	}
}

func getConfigByPipeline(p *pipeline.Pipeline) *Config {
	return p.GetInput().(*Plugin).config
}

// TestWatch tests if watcher notifies about new dirs and files
func TestWatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skip test in short mode")
	}

	iterations := 4
	eventsPerIteration := 2
	finalEvent := 1
	eventCount := iterations*eventsPerIteration + finalEvent
	content := "666\n"
	file := ""

	run(&test.Case{
		Prepare: func() {
			file = createTempFile()
			addString(file, content, true, true)
		},
		Act: func(p *pipeline.Pipeline) {
			for x := 0; x < iterations; x++ {
				dir := fmt.Sprintf("dir_%d", x)
				go func(dir string) {
					dir = filepath.Join(filepath.Dir(file), dir)
					_ = os.Mkdir(dir, 0o770)

					err := os.WriteFile(filepath.Join(dir, "new_file"), []byte(content), perm)
					if err != nil {
						panic(err.Error())
					}
					err = os.WriteFile(filepath.Join(dir, "other_file"), []byte(content), perm)
					if err != nil {
						panic(err.Error())
					}
				}(dir)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal(), "wrong event count")
		},
	}, eventCount)
}

// TestReadSimple tests if file reading works right in the simple case
func TestReadSimple(t *testing.T) {
	eventCount := 5
	events := make([]string, 0)

	run(&test.Case{
		Prepare: func() {
			for i := 0; i < eventCount; i++ {
				events = append(events, fmt.Sprintf(`{"field":"value_%d"}`, i))
			}
		},
		Act: func(p *pipeline.Pipeline) {
			file := createTempFile()
			for _, s := range events {
				addString(file, s, true, true)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal(), "wrong event count")
			for i, s := range events {
				assert.Equal(t, s, p.GetEventLogItem(i), "wrong event")
			}
		},
	}, eventCount)
}

// TestReadContinue tests if file reading works right after restart of the pipeline
func TestReadContinue(t *testing.T) {
	blockSize := 2000
	stopAfter := 100
	processed := 0
	inputEvents := make(map[string]bool, blockSize*2)
	outputEvents := make(map[string]bool, blockSize*2+stopAfter)
	file := ""
	size := 0

	run(&test.Case{
		Prepare: func() {
		},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			for x := 0; x < blockSize; x++ {
				line := fmt.Sprintf(`{"data_1":"line_%d"}`, x)
				size += len(line) + newLine
				inputEvents[line] = true
				addString(file, line, true, false)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			processed = p.GetEventsTotal()
			for i := 0; i < processed; i++ {
				outputEvents[p.GetEventLogItem(i)] = true
			}
		},
	}, stopAfter)

	// restart
	offsetFiles = make(map[string]string)

	run(&test.Case{
		Prepare: func() {
		},
		Act: func(p *pipeline.Pipeline) {
			for x := 0; x < blockSize; x++ {
				line := fmt.Sprintf(`{"data_2":"line_%d"}`, x)
				size += len(line) + newLine
				inputEvents[line] = true
				addString(file, line, true, false)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			for i := 0; i < p.GetEventsTotal(); i++ {
				outputEvents[p.GetEventLogItem(i)] = true
			}

			// we compare maps because we can tolerate  dublicates
			require.Equalf(
				t, inputEvents, outputEvents,
				"input events not equal output events (input len=%d, output len=%d)",
				len(inputEvents), len(outputEvents),
			)

			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, blockSize+blockSize-processed, "dirty")
}

// TestOffsetsSaveSimple tests if offsets saving works right in the simple case
func TestOffsetsSaveSimple(t *testing.T) {
	eventCount := 5
	events := make([]string, 0)
	file := ""
	size := 0

	run(&test.Case{
		Prepare: func() {
			s := "1"
			for i := 0; i < eventCount; i++ {
				s += "1"
				event := fmt.Sprintf(`{"field":"value_%s"}`, s)
				events = append(events, event)
				size += len(event) + newLine
			}
		},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			for _, s := range events {
				addString(file, s, true, true)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestOffsetsSaveContinue tests if plugin skips partial data in the case pipeline starts in the middle of the line
func TestOffsetsSaveContinue(t *testing.T) {
	leftPart := `["left_part",`
	rightPart := `"right_part"]`
	secondLine := `"line_2"`
	size := len(leftPart) + len(rightPart) + newLine + len(secondLine) + newLine
	file := ""

	run(&test.Case{
		Prepare: func() {
			file = createTempFile()
			addString(file, leftPart, false, false)
		},
		Act: func(p *pipeline.Pipeline) {
			addString(file, rightPart, true, false)
			addString(file, secondLine, true, false)
			time.Sleep(100 * time.Millisecond)
		},
		Assert: func(p *pipeline.Pipeline) {
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, 1, "tail")
}

// TestOffsetsLoad tests if plugin skips lines which is located before loaded offsets
func TestOffsetsLoad(t *testing.T) {
	line1 := `{"some key1":"some data"}`
	line2 := `{"some key2":"some data"}`

	run(&test.Case{
		Prepare: func() {
			file := createTempFile()
			addString(file, line1, true, false)
			addString(file, line2, true, false)

			offsetFile := createOffsetFile()
			offsets := genOffsetsContent(file, len(line1))
			addBytes(offsetFile, []byte(offsets), false, false)
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, 1, p.GetEventsTotal(), "wrong event count")
			assert.Equal(t, line2, p.GetEventLogItem(0), "wrong event")
		},
	}, 1)
}

// TestReadLineSequential tests if plugin read works right in the case of sequential data appending to the single line
func TestReadLineSequential(t *testing.T) {
	file := ""
	parts := []string{`["some",`, `"sequential",`, `"data"]`}
	size := len(strings.Join(parts, "")) + newLine

	run(&test.Case{
		Prepare: func() {
			file = createTempFile()
			for _, s := range parts {
				addString(file, s, false, true)
			}
			addString(file, "", true, true)
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, 1, p.GetEventsTotal(), "wrong event count")
			assert.Equal(t, strings.Join(parts, ""), p.GetEventLogItem(0), "wrong event content")
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, 1)
}

// TestReadBufferOverflow tests if plugin read works right in the case line is bigger than read buffer
func TestReadBufferOverflow(t *testing.T) {
	file := ""
	overhead := 128
	iterations := 5
	linesPerIterations := 2

	config := &Config{
		WatchingDir: "./",
		OffsetsFile: "offset.yaml",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})
	firstLine := `"`
	for i := 0; i < config.ReadBufferSize+overhead; i++ {
		firstLine += "a"
	}
	firstLine += `"`

	secondLine := "666"

	size := (len(firstLine) + newLine + len(secondLine) + newLine) * iterations
	eventCount := iterations * linesPerIterations

	run(&test.Case{
		Prepare: func() {
			file = createTempFile()
			for i := 0; i < iterations; i++ {
				addString(file, firstLine+"\n"+secondLine+"\n", false, false)
			}
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal(), "wrong event count")
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadManyCharsRace tests if plugin doesn't have race conditions in the case of sequential processing of chars of single line
func TestReadManyCharsRace(t *testing.T) {
	file := ""
	charCount := 1024
	strQuotes := 2
	size := charCount + strQuotes + newLine
	run(&test.Case{
		Prepare: func() {
			file = createTempFile()

			addString(file, `"`, false, false)
			for i := 0; i < charCount; i++ {
				addString(file, "a", false, false)
			}
			addString(file, `"`, true, false)
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, 1, p.GetEventsTotal(), "wrong event count")
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, 1)
}

// TestReadManyLinesRace tests if plugin doesn't have race conditions in the case of sequential processing of lines
func TestReadManyLinesRace(t *testing.T) {
	eventCount := 1024
	size := 0
	file := ""
	run(&test.Case{
		Prepare: func() {
			file = createTempFile()

			size = addLines(file, eventCount, eventCount*2)
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal(), "wrong event count")
			assertOffsetsAreEqual(t, genOffsetsContent(file, size), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadManyFilesRace tests if plugin doesn't have race conditions in the case of sequential processing of files
func TestReadManyFilesRace(t *testing.T) {
	fileCount := 64
	eventCountPerFile := 128
	eventCount := fileCount * eventCountPerFile
	oneFileSize := 0
	files := make([]string, 0, fileCount)
	run(&test.Case{
		Prepare: func() {
			for i := 0; i < fileCount; i++ {
				file := createTempFile()
				oneFileSize = addLines(file, eventCountPerFile, eventCountPerFile*2)

				files = append(files, file)
			}
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal(), "wrong events count")
			assertOffsetsAreEqual(t, genOffsetsContentMultiple(files, oneFileSize), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadLongJSON tests if plugin read works right if it's super long json
func TestReadLongJSON(t *testing.T) {
	eventCount := 10
	file := ""
	json := getContentBytes("../../../testdata/json/heavy.json")
	logger.Infof("len=%d", len(json))
	run(&test.Case{
		Prepare: func() {
		},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			for i := 0; i < eventCount; i++ {
				addBytes(file, json, false, true)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal())
			assertOffsetsAreEqual(t, genOffsetsContent(file, len(json)*eventCount), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount, "reset")
}

// TestReadManyFilesParallelRace tests if plugin doesn't have race conditions in the case of parallel processing of files
func TestReadManyFilesParallelRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skip test in short mode")
	}
	lineCount := 2
	blockCount := 256
	fileCount := 32
	eventCount := lineCount * fileCount * blockCount
	fds := make([]*os.File, 0, fileCount)
	files := make([]string, 0, fileCount)
	json := getContentBytes("../../../testdata/json/light.json")

	run(&test.Case{
		Prepare: func() {
			for f := 0; f < fileCount; f++ {
				file := createTempFile()
				f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, perm)
				if err != nil {
					panic(err.Error())
				}
				fds = append(fds, f)
				files = append(files, file)
			}
		},
		Act: func(p *pipeline.Pipeline) {
			for i := range fds {
				go func(index int) {
					for i := 0; i < blockCount; i++ {
						addDataFile(fds[index], json)
					}
				}(i)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal())
			assertOffsetsAreEqual(t, genOffsetsContentMultiple(files, len(json)*blockCount), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadManyCharsParallelRace tests if plugin doesn't have race conditions in the case of parallel processing of chars
func TestReadManyCharsParallelRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skip test in short mode")
	}
	config := &Config{
		WatchingDir: "./",
		OffsetsFile: "offsets.yaml",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1, "capacity": 64})

	overhead := 100
	s := ""
	for i := 0; i < config.ReadBufferSize+overhead; i++ {
		s += "a"
	}
	json1 := []byte(fmt.Sprintf(`{"data":"%s"}`+"\n"+`{"data":"666"}`+"\n", s))
	json2 := []byte(fmt.Sprintf(`{"data":"666"}` + "\n"))
	lineCount := 3
	blockCount := 128
	fileCount := 8
	fds := make([]*os.File, 0, fileCount)
	files := make([]string, 0, fileCount)

	eventCount := lineCount * blockCount * fileCount
	run(&test.Case{
		Prepare: func() {
			for f := 0; f < fileCount; f++ {
				file := createTempFile()
				f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, perm)
				if err != nil {
					panic(err.Error())
				}
				fds = append(fds, f)
				files = append(files, file)
			}
		},
		Act: func(p *pipeline.Pipeline) {
			for i := range fds {
				go func(index int) {
					for i := 0; i < blockCount; i++ {
						addDataFile(fds[index], json1)
						addDataFile(fds[index], json2)
					}
				}(i)
			}
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal())
			assertOffsetsAreEqual(t, genOffsetsContentMultiple(files, (len(json1)+len(json2))*blockCount), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadManyPreparedFilesRace tests if plugin doesn't have race conditions in the case of parallel processing of prepared files
func TestReadManyPreparedFilesRace(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}

	lineCount := 2
	blockCount := 128 * 128
	fileCount := 32
	files := make([]string, 0, fileCount)
	eventCount := lineCount * blockCount * fileCount

	json := getContentBytes("../../../testdata/json/light.json")
	run(&test.Case{
		Prepare: func() {
			content := make([]byte, 0, len(json)*blockCount)
			for i := 0; i < blockCount; i++ {
				content = append(content, json...)
			}

			for f := 0; f < fileCount; f++ {
				file := createTempFile()
				files = append(files, file)
				addBytes(file, content, false, false)
			}
		},
		Act: func(p *pipeline.Pipeline) {
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal())
			assertOffsetsAreEqual(t, genOffsetsContentMultiple(files, len(json)*blockCount), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

// TestReadStreamRace tests if plugin doesn't have race conditions in the case of parallel processing of prepared files with streams
func TestReadStreamRace(t *testing.T) {
	linesCount := 2
	blocksCount := 128
	filesCount := 16
	fileNames := make([]string, 0, filesCount)
	eventCount := linesCount * blocksCount * filesCount
	json := getContentBytes("../../../testdata/json/streams.json")

	run(&test.Case{
		Prepare: func() {
			content := make([]byte, 0, len(json)*blocksCount)
			for i := 0; i < blocksCount; i++ {
				content = append(content, json...)
			}

			for f := 0; f < filesCount; f++ {
				file := createTempFile()
				fileNames = append(fileNames, file)
				addBytes(file, content, false, false)
			}
		},
		Act: func(p *pipeline.Pipeline) {
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, eventCount, p.GetEventsTotal())
			assertOffsetsAreEqual(t, genOffsetsContentMultipleStreams(fileNames, len(json)*blocksCount, len(json)*blocksCount-len(json)/2), getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, eventCount)
}

func TestRotationRenameSimple(t *testing.T) {
	if testing.Short() {
		t.Skip("skip test in short mode")
	}
	file := ""
	newFile := ""
	run(&test.Case{
		Prepare: func() {},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			addBytes(file, []byte(`{"Data":"Line1_1"}`), true, false)
			addBytes(file, []byte(`{"Data":"Line2_1"}`), true, false)

			newFile = file + ".new"
			renameFile(file, newFile)
			createFile(file)

			addBytes(newFile, []byte(`{"Data":"Line3_1"}`), true, false)
			addBytes(newFile, []byte(`{"Data":"Line4_1"}`), true, false)
			addBytes(newFile, []byte(`{"Data":"Line5_1"}`), true, false)
			addBytes(newFile, []byte(`{"Data":"Line6_1"}`), true, false)

			addBytes(file, []byte(`{"Data":"Line1_2"}`), true, false)
			addBytes(file, []byte(`{"Data":"Line2_2"}`), true, false)
			addBytes(file, []byte(`{"Data":"Line2_2"}`), true, false)
			addBytes(file, []byte(`{"Data":"Line3_2"}`), true, false)
		},
		Assert: func(p *pipeline.Pipeline) {
			offsets := fmt.Sprintf(`- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: 114
- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: 76
`, newFile, getInodeByFile(newFile), getSourceID(newFile), file, getInodeByFile(file), getSourceID(file))
			assert.Equal(t, 10, p.GetEventsTotal(), "wrong events count")
			assertOffsetsAreEqual(t, offsets, getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, 10)
}

func TestRotationRenameWhileNotWorking(t *testing.T) {
	file := ""
	before := 0
	run(&test.Case{
		Prepare: func() {
		},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			addString(file, `"file_1_line_1"`, true, false)
			addString(file, `"file_1_line_2"`, true, false)
		},
		Assert: func(p *pipeline.Pipeline) {
			before = p.GetEventsTotal()
			assert.Equal(t, 2, p.GetEventsTotal(), "wrong events count")
		},
	}, 2)

	// restart
	offsetFiles = make(map[string]string)
	newFile := rotateFile(file)

	run(&test.Case{
		Prepare: func() {
			addString(newFile, `"file_1_line_3"`, true, false)
			addString(newFile, `"file_1_line_4"`, true, false)
			addString(newFile, `"file_1_line_5"`, true, false)
			addString(newFile, `"file_1_line_6"`, true, false)

			addString(file, `"file_2_line_1"`, true, false)
			addString(file, `"file_2_line_2"`, true, false)
			addString(file, `"file_2_line_3"`, true, false)
			addString(file, `"file_2_line_4"`, true, false)
		},
		Act: func(p *pipeline.Pipeline) {},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, 10, before+p.GetEventsTotal(), "wrong events count")

			contentFormat := `- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: 96
- file: %s
  inode: %d
  source_id: %d
  streams:
    not_set: 64
`
			offsets := fmt.Sprintf(contentFormat, newFile, getInodeByFile(newFile), getSourceID(newFile), file, getInodeByFile(file), getSourceID(file))
			assertOffsetsAreEqual(t, offsets, getContent(getConfigByPipeline(p).OffsetsFile))
		},
	}, 8, "dirty")
}

func TestTruncation(t *testing.T) {
	file := ""
	x := atomic.NewInt32(3)
	run(&test.Case{
		Prepare: func() {},
		Act: func(p *pipeline.Pipeline) {
			file = createTempFile()
			addString(file, `"line_1"`, true, false)
			addString(file, `"line_2"`, true, false)
			addString(file, `"line_3"`, true, true)

			test.WaitForEvents(x)
			truncateFile(file)

			addString(file, `"line_4"`, true, true)
			addString(file, `"line_5"`, true, true)
		},
		Assert: func(p *pipeline.Pipeline) {
			assert.Equal(t, 5, p.GetEventsTotal(), "wrong events count")
			assertOffsetsAreEqual(t, genOffsetsContent(file, (len(`"line_1"`)+newLine)*2), getContent(getConfigByPipeline(p).OffsetsFile))
		},
		Out: func(event *pipeline.Event) {
			logger.Errorf("event=%v", event)
			x.Dec()
		},
	}, 5)
}

func TestTruncationSeq(t *testing.T) {
	if testing.Short() {
		t.Skip("skip long tests in short mode")
	}
	setupDirs()
	defer cleanUp()
	p, _, _ := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	p.Start()

	data := getContentBytes("../../../testdata/json/streams.json")
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	data = append(data, data...)
	truncationSize := int(32 * units.Mebibyte)
	truncationCount := 4

	wg := &sync.WaitGroup{}

	for k := 0; k < 4; k++ {
		wg.Add(1)
		go func() {
			lwg := &sync.WaitGroup{}
			truncations := atomic.Int32{}
			size := atomic.Int32{}
			name := createTempFile()
			file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, perm)
			if err != nil {
				panic(err.Error())
			}
			defer closeFile(file)
			lwg.Add(2)
			go func() {
				for {
					_, _ = file.Write(data)
					size.Add(int32(len(data)))
					if rand.Int()%300 == 0 {
						time.Sleep(time.Millisecond * 200)
					}
					if int(truncations.Load()) > truncationCount {
						break
					}
				}
				lwg.Done()
			}()

			go func() {
				for {
					time.Sleep(50 * time.Millisecond)
					if size.Load() > int32(truncationSize) {
						size.Swap(0)
						_ = file.Truncate(0)
						if int(truncations.Inc()) > truncationCount {
							break
						}
					}
				}
				lwg.Done()
			}()

			lwg.Wait()
			wg.Done()
		}()
	}

	wg.Wait()
	p.Stop()
}

// func TestRenameRotationInsane(t *testing.T) {
//	p, _, _ := test.NewPipelineMock(nil, "passive")
//	p.SetInput(getInputInfo())
//	input := p.GetInput().(*Plugin)
//	output := p.GetOutput().(*devnull.Plugin)
//	p.Start()
//
//	filesCount := 16
//	wg := sync.WaitGroup{}
//	wg.Add(filesCount * 8)
//	output.SetOutFn(func(event *pipeline.Event) {
//		wg.Done()
//	})
//
//	files := make([]string, 0, filesCount)
//
//	for i := 0; i < filesCount; i++ {
//		files = append(files, createTempFile())
//		go func(file string, index int) {
//			addBytes(file, []byte(`{"Data":"Line1_1"}`), true, false)
//			addBytes(file, []byte(`{"Data":"Line2_1"}`), true, false)
//
//			newFile := file + ".new"
//			renameFile(file, newFile)
//			createFile(file)
//
//			addBytes(file, []byte(`{"Data":"Line3_1"}`), true, false)
//			addBytes(file, []byte(`{"Data":"Line4_1"}`), true, false)
//			addBytes(file, []byte(`{"Data":"Line5_1"}`), true, false)
//			addBytes(file, []byte(`{"Data":"Line6_1"}`), true, false)
//
//			addBytes(newFile, []byte(`{"Data":"Line1_2"}`), true, false)
//			addBytes(newFile, []byte(`{"Data":"Line2_2"}`), true, false)
//		}(files[i], i)
//	}
//
//	for i := 0; i < filesCount; i++ {
//		files = append(files, files[i]+".new")
//	}
//
//	wg.Wait()
//	p.Stop()
//
//	assert.Equal(t, filesCount*8, p.GetEventsTotal(), "wrong events count")
//	assertOffsetsAreEqual(t, genOffsetsContentMultiple(files, 4*19), getContent(input.config.OffsetsFile))
// }

func BenchmarkLightJsonReadPar(b *testing.B) {
	lines := 128 * 64
	files := 256

	if fs, err := os.ReadDir(filesDir); err != nil || len(fs) == 0 {
		json := getContent("../../../testdata/json/light.json")

		content := make([]byte, 0, len(json)*lines)
		for i := 0; i < lines; i++ {
			content = append(content, json...)
		}

		for f := 0; f < files; f++ {
			file := createTempFile()
			addBytes(file, content, false, false)
		}
		logger.Infof("%s", filesDir)

		bytes := int64(files * lines * len(json))
		logger.Infof("will read %dMb", bytes/1024/1024)
		b.SetBytes(bytes)
	}

	opts := []string{"passive", "perf"}
	if useLowMem, _ := strconv.ParseBool(os.Getenv("POOL_LOW_MEMORY")); useLowMem {
		b.Log("using low memory pool")
		opts = append(opts, "use_pool_low_memory")
	}

	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		p, _, output := test.NewPipelineMock(nil, opts...)

		offsetsDir = b.TempDir()
		p.SetInput(getInputInfo())

		wg := &sync.WaitGroup{}
		wg.Add(lines * files * 2)

		output.SetOutFn(func(event *pipeline.Event) {
			wg.Done()
		})

		p.Start()

		b.StartTimer()
		wg.Wait()
		b.StopTimer()

		p.Stop()
	}
}
