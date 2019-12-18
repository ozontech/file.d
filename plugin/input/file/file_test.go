package file

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/alecthomas/units"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/logger"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
	"go.uber.org/atomic"
)

var (
	filesDir   = ""
	offsetsDir = ""
)

const offsetsFile = "filed_offsets.yaml"

func TestMain(m *testing.M) {
	setupDirs()
	exitVal := m.Run()
	cleanUp()
	os.Exit(exitVal)
}

func setupDirs() {
	f, err := ioutil.TempDir("", "input_file")
	if err != nil {
		panic(err.Error())
	}
	filesDir = f

	f, err = ioutil.TempDir("", "input_file_offsets")
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
}

func stdConfig() *Config {
	return &Config{WatchingDir: filesDir, OffsetsFile: filepath.Join(offsetsDir, offsetsFile), PersistenceMode: "async"}
}

func startStdCleanPipeline() (*pipeline.Pipeline, *Plugin, *devnull.Plugin) {
	cleanUp()
	setupDirs()

	return startStdPipeline()
}

func startStdPipeline() (*pipeline.Pipeline, *Plugin, *devnull.Plugin) {
	return startPipeline(stdConfig(), true)
}

func startBenchPipeline() (*pipeline.Pipeline, *Plugin, *devnull.Plugin) {
	return startPipeline(stdConfig(), false)
}

func startPipeline(config *Config, enableEventLog bool) (*pipeline.Pipeline, *Plugin, *devnull.Plugin) {
	p := pipeline.NewTestPipeLine(true)
	if enableEventLog {
		p.EnableEventLog()
	}

	anyPlugin, _ := Factory()
	inputPlugin := anyPlugin.(*Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func renameFile(oldFile string, newFile string) {
	err := os.Rename(oldFile, newFile)
	if err != nil {
		panic(err.Error())
	}
}

func closeFile(f *os.File) {
	err := f.Close()
	if err != nil {
		panic(err.Error())
	}
}

func truncateFile(file string) {
	f, err := os.OpenFile(file, os.O_WRONLY, 0664)
	if err != nil {
		panic(err.Error())
	}
	defer closeFile(f)

	err = f.Truncate(0)
	if err != nil {
		panic(err.Error())
	}
}

//func waitForFile(file string) {
//	size := int64(0)
//	t := time.Now()
//	for {
//		stat, err := os.Stat(file)
//		if err != nil {
//			panic(err.Error())
//		}
//
//		if stat.Size() != size {
//			t = time.Now()
//			size = stat.Size()
//		}
//
//		if time.Now().Sub(t) > time.Millisecond*100 {
//			return
//		}
//	}
//}

//func rotateFile(file string) string {
//	newFile := file + ".new"
//	renameFile(file, newFile)
//	createFile(file)
//
//	return newFile
//}

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
	if _, err := file.Write(data);
		err != nil {
		panic(err.Error())
	}
}

func addData(file string, data []byte, isLine bool, sync bool) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
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

	if sync {
		err = f.Sync()
		if err != nil {
			panic(err.Error())
		}
	}
}

func addLines(file string, from int, to int) {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		panic(err.Error())
	}

	defer closeFile(f)

	for i := from; i < to; i++ {

		if _, err = f.WriteString(fmt.Sprintf(`"_`)); err != nil {
			panic(err.Error())
		}
		if _, err = f.WriteString(fmt.Sprintf(`%d"`+"\n", i)); err != nil {
			panic(err.Error())
		}
	}

	err = f.Sync()
	if err != nil {
		panic(err.Error())
	}
}

func getContent(file string) string {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return string(content)
}

func getContentBytes(file string) []byte {
	content, err := ioutil.ReadFile(file)
	if err != nil {
		panic(err)
	}

	return content
}

func genOffsetsContent(file string, offset int) string {
	return fmt.Sprintf(`- file: %d %s
  not_set: %d
`, getInodeByFile(file), file, offset)
}

func genOffsetsContentMultiple(files []string, offset int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %d %s
  not_set: %d
`, getInodeByFile(file), file, offset)...)
	}

	return string(result)
}

func genOffsetsContentMultipleStreams(files []string, offsetStdErr int, offsetStdOut int) string {
	result := make([]byte, 0, len(files)*100)
	for _, file := range files {
		result = append(result, fmt.Sprintf(`- file: %d %s
  stderr: %d
  stdout: %d
`, getInodeByFile(file), file, offsetStdErr, offsetStdOut)...)
	}

	return string(result)
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

func assertOffsetsEqual(t *testing.T, offsetsContentA string, offsetsContentB string) {
	offsetDB := newOffsetDB("", "")
	offsetsA := offsetDB.parse(offsetsContentA)
	offsetsB := offsetDB.parse(offsetsContentB)
	for sourceID, inode := range offsetsA {
		_, has := offsetsB[sourceID]
		assert.True(t, has, "offsets aren't equal, source id=%d", sourceID)
		for stream, offset := range inode.streams {
			offsetB, has := offsetsB[sourceID].streams[stream]
			assert.True(t, has, "offsets aren't equal, no stream %q", stream)
			assert.Equal(t, offset, offsetB, "offsets aren't equal")
		}
	}
}

func TestWatch(t *testing.T) {
	c, _, _ := startStdCleanPipeline()

	file := createTempFile()
	content := []byte(`{"key":"value"}` + "\n")

	time.Sleep(time.Millisecond * 500)
	dir := filepath.Join(filepath.Dir(file), "dir1")
	_ = os.Mkdir(dir, 0770)
	time.Sleep(time.Millisecond * 500)

	err := ioutil.WriteFile(filepath.Join(dir, "new_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(filepath.Join(dir, "other_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	time.Sleep(time.Millisecond * 500)
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err.Error())
	}

	dir = filepath.Join(filepath.Dir(file), "dir2")
	_ = os.Mkdir(dir, 0770)
	time.Sleep(time.Millisecond * 500)

	err = ioutil.WriteFile(filepath.Join(dir, "new_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	err = ioutil.WriteFile(filepath.Join(dir, "other_file"), content, 0660)
	if err != nil {
		panic(err.Error())
	}
	time.Sleep(time.Millisecond * 500)
	err = os.RemoveAll(dir)
	if err != nil {
		panic(err.Error())
	}

	addData(file, content, true, true)

	c.Stop()

	assert.Equal(t, 5, c.GetEventsTotal(), "wrong event count")
}

func TestReadLineSimple(t *testing.T) {
	pipe, _, output := startStdCleanPipeline()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, true)
	addData(file, []byte(`{"Data":"Line2"}`), true, true)
	addData(file, []byte(`{"Data":"Line3"}`), true, true)

	output.WaitFor(3)
	pipe.Stop()

	assert.Equal(t, 3, pipe.GetEventsTotal(), "wrong event count")
	assert.Equal(t, `{"Data":"Line1"}`, pipe.GetEventLogItem(0), "wrong event content")
	assert.Equal(t, `{"Data":"Line2"}`, pipe.GetEventLogItem(1), "wrong event content")
	assert.Equal(t, `{"Data":"Line3"}`, pipe.GetEventLogItem(2), "wrong event content")
}

func TestOffsetsSaveSimple(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)

	output.WaitFor(3)
	pipe.Stop()

	assert.Equal(t, genOffsetsContent(file, 51), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestOffsetsSaveAfterStartInTheMiddleOfFile(t *testing.T) {
	file := createTempFile()
	addData(file, []byte(`{"Data":`), false, false)

	config := &Config{WatchingDir: filesDir, OffsetsFile: filepath.Join(offsetsDir, offsetsFile), PersistenceMode: "sync", OffsetsOp: "tail"}
	pipe, input, output := startPipeline(config, true)

	addData(file, []byte(`"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)

	output.WaitFor(1)
	pipe.Stop()

	assert.Equal(t, genOffsetsContent(file, 34), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestOffsetsLoad(t *testing.T) {
	cleanUp()
	setupDirs()

	data1 := `{"some key1":"some data"}`
	data2 := `{"some key2":"some data"}`
	dataFile := createTempFile()
	addData(dataFile, []byte(data1), true, false)
	addData(dataFile, []byte(data2), true, false)

	offsetFile := createOffsetFile()
	offsets := genOffsetsContent(dataFile, len(data1))
	addData(offsetFile, []byte(offsets), false, false)

	pipe, input, output := startStdPipeline()

	output.WaitFor(1)

	// force save into another file
	input.jobProvider.offsetDB.offsetsFile += ".new"
	pipe.Stop()

	assert.Equal(t, 1, pipe.GetEventsTotal(), "wrong event count")
	assert.Equal(t, data2, pipe.GetEventLogItem(0), "wrong event content")
}

func TestContinueReading(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3"}`), true, false)

	pipe.Stop()

	processed := pipe.GetEventsTotal()

	addData(file, []byte(`{"Data":"Line4"}`), true, false)
	addData(file, []byte(`{"Data":"Line5"}`), true, false)
	addData(file, []byte(`{"Data":"Line6"}`), true, false)
	addData(file, []byte(`{"Data":"Line7"}`), true, false)

	pipe, input, output = startStdPipeline()

	output.WaitFor(7 - processed)
	pipe.Stop()

	assert.Equal(t, 7, processed+pipe.GetEventsTotal(), "wrong event count")
	assert.Equal(t, `{"Data":"Line7"}`, pipe.GetEventLogItem(pipe.GetEventsTotal()-1), "wrong event")
	assert.Equal(t, genOffsetsContent(file, 119), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestReadSeq(t *testing.T) {
	c, p, output := startStdCleanPipeline()

	file := createTempFile()

	addData(file, []byte(`{"Data":"Line1`), false, true)
	addData(file, []byte(`Line2`), false, true)
	addData(file, []byte(`Line3"}`), true, true)

	output.WaitFor(1)
	c.Stop()

	assert.Equal(t, 1, c.GetEventsTotal(), "wrong event count")
	assert.Equal(t, `{"Data":"Line1Line2Line3"}`, c.GetEventLogItem(0), "wrong event content")
	assert.Equal(t, genOffsetsContent(file, 27), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadLong(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	file := createTempFile()

	s := ""
	overhead := 100
	for i := 0; i < defaultReadBufferSize+overhead; i++ {
		s = s + "a"
	}
	data := fmt.Sprintf(`{"Data":"%s"}`+"\n"+`{"Data":"xxx"}`, s)

	addData(file, []byte(data), true, false)
	addData(file, []byte(data), true, false)
	addData(file, []byte(data), true, false)

	output.WaitFor(6)
	pipe.Stop()

	assert.Equal(t, 6, pipe.GetEventsTotal(), "wrong event count")
	assert.Equal(t, genOffsetsContent(file, (defaultReadBufferSize+overhead+27)*3), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestReadComplexSeqMulti(t *testing.T) {
	c, p, _ := startStdCleanPipeline()

	file := createTempFile()

	lines := 1000
	addLines(file, lines, lines+lines)

	c.Stop()

	assert.Equal(t, lines, c.GetEventsTotal(), "wrong events count")
	assert.Equal(t, genOffsetsContent(file, lines*8), getContent(p.config.OffsetsFile), "wrong offsets")
}

func TestReadOneLineSequential(t *testing.T) {
	plugin, input, output := startStdCleanPipeline()

	file := createTempFile()

	addData(file, []byte(`"`), false, false)
	charsCount := 100
	for i := 0; i < charsCount; i++ {
		addData(file, []byte{'a'}, false, false)
	}
	addData(file, []byte{}, false, false)
	addData(file, []byte(`"`), true, false)

	output.WaitFor(1)
	plugin.Stop()

	assert.Equal(t, 1, plugin.GetEventsTotal(), "wrong events count")
	assert.Equal(t, charsCount+2, len(plugin.GetEventLogItem(0)), "wrong event")
	assert.Equal(t, genOffsetsContent(file, charsCount+3), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestReadManyLinesInParallel(t *testing.T) {
	cleanUp()
	setupDirs()

	linesCount := 100
	filesCount := 60
	filesNames := make([]string, 0, filesCount)
	for i := 0; i < filesCount; i++ {
		file := createTempFile()
		addLines(file, linesCount, linesCount+linesCount)

		filesNames = append(filesNames, file)
	}

	pipe, input, output := startStdPipeline()

	output.WaitFor(linesCount * filesCount)
	pipe.Stop()

	assert.Equal(t, linesCount*filesCount, pipe.GetEventsTotal(), "wrong events count")
	assertOffsetsEqual(t, genOffsetsContentMultiple(filesNames, linesCount*7), getContent(input.config.OffsetsFile))
}

func TestReadHeavyJSON(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	file := createTempFile()
	json := getContentBytes("../../../testdata/json/heavy.json")
	lines := 10
	for i := 0; i < lines; i++ {
		addData(file, json, false, true)
	}

	output.WaitFor(lines)
	pipe.Stop()
	assert.Equal(t, lines, pipe.GetEventsTotal())
	assert.Equal(t, genOffsetsContent(file, len(json)*lines), getContent(input.config.OffsetsFile), "wrong offsets")
}

func TestReadManyFilesInParallel(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	linesCount := 2
	blocksCount := 256
	filesCount := 32
	files := make([]*os.File, 0, filesCount)
	fileNames := make([]string, 0, filesCount)
	json := getContentBytes("../../../testdata/json/light.json")

	for f := 0; f < filesCount; f++ {
		file := createTempFile()
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			panic(err.Error())
		}
		files = append(files, f)
		fileNames = append(fileNames, file)
	}

	for i := range files {
		go func(index int) {
			for i := 0; i < blocksCount; i++ {
				addDataFile(files[index], json)
			}
		}(i)
	}

	output.WaitFor(linesCount * filesCount * blocksCount)
	pipe.Stop()

	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, len(json)*blocksCount), getContent(input.config.OffsetsFile))
}

func TestReadInsaneLong(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	s := ""
	overhead := 100
	for i := 0; i < defaultReadBufferSize+overhead; i++ {
		s = s + "a"
	}
	json1 := []byte(fmt.Sprintf(`{"Data":"%s"}`+"\n"+`{"Data":"xxx"}`+"\n", s))
	json2 := []byte(fmt.Sprintf(`{"Data":"xxx"}` + "\n"))

	linesCount := 3
	blocksCount := 128
	filesCount := 8
	files := make([]*os.File, 0, filesCount)
	fileNames := make([]string, 0, filesCount)

	for f := 0; f < filesCount; f++ {
		file := createTempFile()
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0660)
		if err != nil {
			panic(err.Error())
		}
		files = append(files, f)
		fileNames = append(fileNames, file)
	}

	for i := range files {
		go func(index int) {
			for i := 0; i < blocksCount; i++ {
				addDataFile(files[index], json1)
				addDataFile(files[index], json2)
			}
		}(i)
	}

	output.WaitFor(linesCount * blocksCount * filesCount)
	pipe.Stop()

	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, (len(json1)+len(json2))*blocksCount), getContent(input.config.OffsetsFile))
}

func TestReadManyPreparedFilesInParallel(t *testing.T) {
	cleanUp()
	setupDirs()

	linesCount := 2
	blocksCount := 128 * 128
	filesCount := 32

	json := getContentBytes("../../../testdata/json/light.json")
	content := make([]byte, 0, len(json)*blocksCount)
	for i := 0; i < blocksCount; i++ {
		content = append(content, json...)
	}

	fileNames := make([]string, 0, filesCount)
	for f := 0; f < filesCount; f++ {
		file := createTempFile()
		fileNames = append(fileNames, file)
		addData(file, content, false, false)
	}

	pipe, input, output := startStdPipeline()
	output.WaitFor(linesCount * blocksCount * filesCount)
	pipe.Stop()

	assertOffsetsEqual(t, genOffsetsContentMultiple(fileNames, len(json)*blocksCount), getContent(input.config.OffsetsFile))
}

func TestReadManyPreparedFilesInParallelWithStreams(t *testing.T) {
	cleanUp()
	setupDirs()

	linesCount := 2
	blocksCount := 128
	filesCount := 16

	json := getContentBytes("../../../testdata/json/streams.json")
	content := make([]byte, 0, len(json)*blocksCount)
	for i := 0; i < blocksCount; i++ {
		content = append(content, json...)
	}

	fileNames := make([]string, 0, filesCount)
	for f := 0; f < filesCount; f++ {
		file := createTempFile()
		fileNames = append(fileNames, file)
		addData(file, content, false, false)
	}

	pipe, input, output := startStdPipeline()
	output.WaitFor(linesCount * blocksCount * filesCount)
	pipe.Stop()

	assertOffsetsEqual(t, genOffsetsContentMultipleStreams(fileNames, len(json)*blocksCount, len(json)*blocksCount-len(json)/2), getContent(input.config.OffsetsFile))
}

func TestRenameRotation(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	file := createTempFile()
	addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_1"}`), true, false)

	newFile := file + ".new"
	renameFile(file, newFile)
	createFile(file)

	addData(newFile, []byte(`{"Data":"Line3_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line4_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line5_1"}`), true, false)
	addData(newFile, []byte(`{"Data":"Line6_1"}`), true, false)

	addData(file, []byte(`{"Data":"Line1_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
	addData(file, []byte(`{"Data":"Line3_2"}`), true, false)

	output.WaitFor(10)
	pipe.Stop()

	offsets := fmt.Sprintf(`- file: %d %s
  not_set: 114
- file: %d %s
  not_set: 76
`, getInodeByFile(newFile), newFile, getInodeByFile(file), file)
	assert.Equal(t, 10, pipe.GetEventsTotal(), "wrong events count")
	assertOffsetsEqual(t, offsets, getContent(input.config.OffsetsFile))
}

// todo: this test should work but we should have some "fingerprint" of a file to do it
//func TestShutdownRotation(t *testing.T) {
//	c, p := startPipeline(stdConfig(),true)
//
//	file := createTempFile()
//	addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_1"}`), true, false)
//	c.HandleEventFlowFinish(false)
//
//	c.WaitUntilDone(false)
//	c.Stop()
//	assert.Equal(t, 2, c.GetEventLogLength(), "wrong event count")
//	assert.Equal(t, 2, c.GetEventsTotal(), "wrong events count")
//
//	newFile := rotateFile(file)
//
//	addData(newFile, []byte(`{"Data":"Line3_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line4_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line5_1"}`), true, false)
//	addData(newFile, []byte(`{"Data":"Line6_1"}`), true, false)
//
//	addData(file, []byte(`{"Data":"Line1_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line2_2"}`), true, false)
//	addData(file, []byte(`{"Data":"Line3_2"}`), true, false)
//
//	c, p = startPipeline("async", true, nil)
//	defer c.Stop()
//	c.HandleEventFlowFinish(false)
//	c.WaitUntilDone(false)
//	//
//	offsets := fmt.Sprintf(`- file: %d %s
//  not_set: 114
//- file: %d %s
//  not_set: 76
//`, getInodeByFile(newFile), newFile, getInodeByFile(file), file)
//
//	assert.Equal(t, 8, c.GetEventLogLength(), "wrong event count")
//	assert.Equal(t, 8, c.GetEventsTotal(), "wrong events count")
//
//	assertOffsetsEqual(t, offsets, getContent(p.config.OffsetsFile))
//}

func TestTruncation(t *testing.T) {
	pipe, input, output := startStdCleanPipeline()

	data := []byte(`{"Data":"Line1"}`)
	file := createTempFile()
	addData(file, data, true, false)
	addData(file, data, true, false)

	output.WaitFor(2)
	assert.Equal(t, 2, pipe.GetEventsTotal(), "wrong events count")
	output.ResetWaitFor()

	truncateFile(file)
	data = []byte(`{"Data":"Line2"}`)
	addData(file, data, true, true)
	addData(file, data, true, true)
	addData(file, data, true, true)

	output.WaitFor(3)
	pipe.Stop()

	assert.Equal(t, 5, pipe.GetEventsTotal(), "wrong events count")
	assertOffsetsEqual(t, genOffsetsContent(file, (len(data)+1)*3), getContent(input.config.OffsetsFile))
}

func TestTruncationSeq(t *testing.T) {
	c, _, _ := startStdCleanPipeline()

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
			truncations := 0
			size := atomic.Int32{}
			name := createTempFile()
			file, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0664)
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
					if truncations > truncationCount {
						break
					}
				}
				lwg.Done()
			}()

			go func() {
				for {
					time.Sleep(50 * time.Millisecond)
					if size.Load() > int32(truncationSize) {
						fmt.Println("truncate")
						size.Swap(0)
						_ = file.Truncate(0)
						truncations++
						if truncations > truncationCount {
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
	c.Stop()
}

// todo: remove sleep after file creation and fix test
func TestRenameRotationInsane(t *testing.T) {
	c, p, _ := startStdCleanPipeline()

	filesCount := 16
	files := make([]string, 0, filesCount)

	wg := sync.WaitGroup{}
	wg.Add(filesCount)
	for i := 0; i < filesCount; i++ {
		files = append(files, createTempFile())
		time.Sleep(time.Millisecond * 100)
		go func(file string, index int, wg *sync.WaitGroup) {
			addData(file, []byte(`{"Data":"Line1_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line2_1"}`), true, false)

			newFile := file + ".new"
			renameFile(file, newFile)
			createFile(file)

			addData(file, []byte(`{"Data":"Line3_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line4_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line5_1"}`), true, false)
			addData(file, []byte(`{"Data":"Line6_1"}`), true, false)

			addData(newFile, []byte(`{"Data":"Line1_2"}`), true, false)
			addData(newFile, []byte(`{"Data":"Line2_2"}`), true, false)
			wg.Done()
		}(files[i], i, &wg)
	}

	for i := 0; i < filesCount; i++ {
		files = append(files, files[i]+".new")
	}

	wg.Wait()
	p.jobProvider.maintenanceJobs()
	c.Stop()

	assert.Equal(t, filesCount*8, c.GetEventsTotal(), "wrong events count")
	assertOffsetsEqual(t, genOffsetsContentMultiple(files, 4*19), getContent(p.config.OffsetsFile))
}

func BenchmarkRawRead(b *testing.B) {
	bytes := 100 * 1024 * 1024
	file := createTempFile()
	data := make([]byte, 0, bytes)
	lengthOffset := 300
	lengthRange := 300
	line := make([]byte, 0, lengthOffset+lengthRange+1)
	i := 0
	lines := 0
	for i < bytes {
		l := lengthOffset + rand.Int()%lengthRange
		if i+l > bytes {
			l -= i + l - bytes + 1
		}
		c := line[0:l]
		for j := 0; j < l; j++ {
			c[j] = 'a'
		}
		c[l-1] = '\n'
		lines++
		i += l + 1
		data = append(data, c...)
	}
	addData(file, data, false, true)

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		c, _, _ := startBenchPipeline()

		fmt.Printf("gen lines=%d, processed lines: %d\n", lines, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkHeavyJsonRead(b *testing.B) {
	file := createTempFile()
	json := getContent("../../../testdata/json/heavy.json")
	lines := 100
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}
	addData(file, content, false, false)

	b.ResetTimer()
	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		c, _, _ := startBenchPipeline()

		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkLightJsonReadSeq(b *testing.B) {
	file := createTempFile()
	json := getContent("../../../testdata/json/light.json")
	lines := 30000
	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}
	addData(file, content, false, false)

	b.SetBytes(int64(lines * len(json)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c, _, _ := startBenchPipeline()

		fmt.Printf("gen bytes=%d, gen lines=%d, processed lines: %d\n", lines*len(json), lines*2, c.GetEventsTotal())

		c.Stop()
	}
}

func BenchmarkLightJsonReadPar(b *testing.B) {
	lines := 128 * 128
	files := 64

	json := getContent("../../../testdata/json/light.json")

	content := make([]byte, 0, len(json)*lines)
	for i := 0; i < lines; i++ {
		content = append(content, json...)
	}

	for f := 0; f < files; f++ {
		file := createTempFile()
		addData(file, content, false, false)
	}

	bytes := int64(files * lines * len(json))
	logger.Infof("")
	logger.Infof("will read %dMb", bytes/1024/1024)
	b.SetBytes(bytes)
	b.ReportAllocs()
	b.StopTimer()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		c, _, _ := startBenchPipeline()
		b.StartTimer()
		b.StopTimer()
		c.Stop()
	}
}
